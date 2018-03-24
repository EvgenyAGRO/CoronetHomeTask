package cht.cache;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LRUPersistentCache extends Thread {
	
	private static final Logger logger = Logger.getLogger(
			LRUPersistentCache.class.getSimpleName());
	
	private static final int DEFAULT_MAX_SIZE = 100000;
	private static final int SLEEP_INTERVAL_SEC = 30; // Check every 30 sec how much entries were evicted from cache
	private static final int PERSIST_THRESHOLD = 1000; // If there were more then 1000 evicted we want to persist them to disk and clean

	private volatile boolean _running;
	
	private  ConcurrentLinkedQueue<String> _cacheQueue; // Queue that keeps the order of insertion to the cache.
	private  ConcurrentSkipListMap<String,LinkedList<String>> _cacheMap; // The cache map itself
	private  ConcurrentSkipListMap<String,LinkedList<String>> _recentlyRemovedEntries; // The entries that were removed from cache but not yet serialized
	
	private final ReadWriteLock _readWriteLock;
	private final Lock _readLock;
	private final Lock _writeLock; 
	
	private int _maxSize; // Max cache size
	private int _queueSize; // Current queue size
	private String _filePathForPersistance; // File path for serialization
	
	public LRUPersistentCache(int maxSize, String filePathForPersistance){
		_maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_SIZE;
		_cacheQueue = new ConcurrentLinkedQueue<>();
		_recentlyRemovedEntries = new ConcurrentSkipListMap<>();
		_readWriteLock = new ReentrantReadWriteLock();
		_readLock = _readWriteLock.readLock();
		_writeLock = _readWriteLock.writeLock();
		_running = true;
		_filePathForPersistance = filePathForPersistance;
		_cacheMap = getCacheMapOnStartUp();
		logger.setLevel(Level.WARNING);
	}
	
	public LRUPersistentCache(String filePathForPersistance){
		this(DEFAULT_MAX_SIZE, filePathForPersistance);
	}
	
	/**
	 * Load persisted data on startup(if no new data will arrive, every get will have to read from disk, avoid it) 
	 * */
	private ConcurrentSkipListMap<String,LinkedList<String>> getCacheMapOnStartUp(){
		Map<String, LinkedList<String>> loadedMap =  loadDataFromDisk();
		return loadedMap == null ? new ConcurrentSkipListMap<>() : new ConcurrentSkipListMap<>(loadedMap);
	}
	
	/**
	 * Get value from cache by key 
	 * */
	public List<String> get(String key){
		boolean foundInRecentlyRemoved = false;
		boolean foundInPersistedData = false;

		LinkedList<String> retVal=null;
		
		// We are reading from cache so read lock is enough. Allows parallelization.
		_readLock.lock();
		try {
			
			// If cache map contains key, update its order in queue, it was just used
			// Queue size may not be updated here because remove and add just cancel each other
			if(_cacheMap.containsKey(key)) {
				_cacheQueue.remove(key);
				retVal = _cacheMap.get(key);
				_cacheQueue.add(key);
			}
			else if (_recentlyRemovedEntries.containsKey(key)){
				// If it was not found in cache it could be recently removed
				retVal = _recentlyRemovedEntries.get(key);
				foundInRecentlyRemoved = true;
			}
			else{
				// Otherwise try to find it on disk
				Map<String, LinkedList<String>> loadedMap = loadDataFromDisk();
				if (loadedMap != null && loadedMap.containsKey(key)){
					retVal = loadedMap.get(key);
				}
				foundInPersistedData = true;
			}
						
		} finally {
			_readLock.unlock();
		}
		
		// If it was found either in recently removed or on disk put it back to cache
		if (retVal != null && (foundInPersistedData || foundInRecentlyRemoved)){
			set(key, retVal, false);
			// We put it back in cash so remove from recently removed, in case it was moved to cache from disk
			// next serialization it will be just overwritten so no need to update on disk.
			if (foundInRecentlyRemoved){
				_recentlyRemovedEntries.remove(key);
			}
		}
		
		return retVal;
	}
	
	/**
	 * Set value to cache with specified key mapping
	 * */
	public void set(String key,List<String> value){
		set(key, value, true);
	}
	
	/**
	 * Set value to cache with specified key mapping with slight optimization
	 * */
	private void set(String key,List<String> value, boolean checkExistence){
		
		_writeLock.lock();
		try {
			// Sometimes we simply may know when the key exists/not exists thus no need to check the cache
			if(checkExistence && _cacheMap.containsKey(key)){
				 _cacheQueue.remove(key);
				 _queueSize--;
			}
			
			// While the queue size is bigger then the threshold remove from cache by LRU strategy to recently removed collection
			while(_queueSize >= _maxSize){
				 String queueKey = _cacheQueue.poll();
				 _queueSize--;
				 _recentlyRemovedEntries.put(queueKey, _cacheMap.get(queueKey));
				 _cacheMap.remove(queueKey);
			}
			
			// Update queue and insert key with new value
			_cacheQueue.add(key);
			_queueSize++;
			_cacheMap.put(key, value instanceof LinkedList ? (LinkedList<String>) value : new LinkedList<>(value));			
		} finally{
			_writeLock.unlock();
		}
	}
	
	/**
	 * Add value from right/left to the list associated with the key
	 * */
	private void addFromRightOrLeft(String key, String value, boolean rightAdd){
		_writeLock.lock();
		try {
			
			LinkedList<String> tmpList = null;
			
			if(_cacheMap.containsKey(key)){
				_cacheQueue.remove(key);
				_queueSize--;
				tmpList = _cacheMap.get(key);
			} else {
				// If it was not found, simply insert
				tmpList = new LinkedList<>();
				tmpList.add(value);
				set(key, tmpList, false);
				return;
			}
			
			// Add to linked list according to the argument
			if (rightAdd){
				tmpList.addLast(value);
			} else {
				tmpList.addFirst(value);
			}
			
			// Update cache, queue and queue size
			_cacheQueue.add(key);
			_queueSize++;
			_cacheMap.put(key, tmpList);
			
		} finally{
			_writeLock.unlock();
		}
	}
	
	/**
	 * Add value from right to the list associated with the key
	 * */
	public void rightAdd(String key,String value) {
		addFromRightOrLeft(key, value, true);
	}
	
	/**
	 * Add value from left to the list associated with the key
	 * */
	public void leftAdd(String key,String value) {
		addFromRightOrLeft(key, value, false);
	}
	
	/**
	 * Get all keys from the cache by prefix
	 * */
	public Set<String> getAllKeys(String pattern) {
		Set<String> retValKeys = new HashSet<>();
		// We do not know if some of the keys with a given prefix were already removed so check in recently removed as well
		Map<String, LinkedList<String>> recentlyRemovedPrefixMap = _recentlyRemovedEntries.subMap(pattern, pattern + Character.MAX_VALUE);
		Map<String, LinkedList<String>> persistedData = null;
		// We do not know if some of the keys with a given prefix were dumped to disk so load it anyway
		persistedData = loadDataFromDisk();
		
		_readLock.lock();
		try {
			
			// Skiplist based map was used for more efficient prefix submap extraction
			Map<String, LinkedList<String>> prefixMap = _cacheMap.subMap(pattern, pattern + Character.MAX_VALUE);
			
			// Update that all extracted keys were just used
			for (Map.Entry<String, LinkedList<String>> entry : prefixMap.entrySet()){
				_cacheQueue.remove(entry.getKey());
				retValKeys.add(entry.getKey());
				_cacheQueue.add(entry.getKey());
			}

		} finally {
			_readLock.unlock();
		}
		
		// Update all recently removed keys and put them back to cache
		for (Map.Entry<String, LinkedList<String>> entry : recentlyRemovedPrefixMap.entrySet()){
			set(entry.getKey(), entry.getValue(), false);
			_recentlyRemovedEntries.remove(entry.getKey());
			retValKeys.add(entry.getKey());
		}

		if (persistedData != null){
			// Update all persisted keys and put them back to cache
			for (Map.Entry<String, LinkedList<String>> entry : persistedData.entrySet()){
				set(entry.getKey(), entry.getValue(), false);
				retValKeys.add(entry.getKey());
			}
		}
		return retValKeys;
	}
	
	/**
	 * Deserialize backed cache on disk.
	 * */
	@SuppressWarnings("unchecked")
	private Map<String, LinkedList<String>> loadDataFromDisk() {
		File persistanceFile = new File(_filePathForPersistance);
		// Check if file exists and is not empty
		if (!persistanceFile.isFile() || persistanceFile.length() == 0){
			return null;
		}
		
		Map<String, LinkedList<String>> loadedMap = null;
		try {
			// Maps already implement Serializable so we may use it
			FileInputStream fileIn = new FileInputStream(_filePathForPersistance);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			loadedMap = (Map<String, LinkedList<String>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			logger.log(
	           Level.WARNING,
	           "IOException occured when reading persisted data from disk.",
	           i.getCause());
			return null;
		} catch (ClassNotFoundException c) {
			logger.log(
	           Level.WARNING,
	           "Unexpected class not found exception during deserialization.",
	           c.getCause());
			return null;
		}
		return loadedMap;
	}
	

	/**
	 * Serialize cache to disk.
	 * */
	private void persistDataToDisk(Map<String, LinkedList<String>> data){
		try {
			// Maps already implement Serializable so we may use it
			FileOutputStream fileOut = new FileOutputStream(_filePathForPersistance);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(data);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			logger.log(
	           Level.WARNING,
	           "IOException occured during serialization to disk.",
	           i.getCause());
	    }
	}

	
	/**
	 * Merge maps for concentrated serialization.
	 * */
	private Map<String,LinkedList<String>> mergeMaps(Map<String, LinkedList<String>> cacheMap, Map<String, LinkedList<String>> recentlyRemovedMap){
		Map<String, LinkedList<String>> mergedMap = Stream.of(cacheMap, recentlyRemovedMap)
			    .flatMap(map -> map.entrySet().stream())
			    .collect(
			        Collectors.toMap(
			            Map.Entry::getKey,
			            Map.Entry::getValue,
			            (v1, v2) -> v1, // If there two maps has same key with different values, just take the value from the first map
			        	() -> new HashMap<>(_cacheMap)
			        )
			    );
		return mergedMap;
	}
	
	
	/**
	 * Save all available data to disk, in case of shutdown.
	 * */
	private void persistAllAvailableData(){
		Map<String,LinkedList<String>> loadedMap = loadDataFromDisk();
		Map<String,LinkedList<String>> mergedLoadedAndRecentlyRemovedMap = loadedMap != null ? mergeMaps(_recentlyRemovedEntries, loadedMap) : _recentlyRemovedEntries;
		Map<String,LinkedList<String>> mergedAll = mergeMaps(_cacheMap, mergedLoadedAndRecentlyRemovedMap);
		persistDataToDisk(mergedAll);
	}

	@Override
	public void run() {
		
		while (_running){
			
			try {
				TimeUnit.SECONDS.sleep(SLEEP_INTERVAL_SEC);
			} catch (InterruptedException e) {
				logger.log(
		           Level.WARNING,
		           "Server is terminating. Persisting data.",
		           e.getCause());
				
				persistAllAvailableData();
				break;
			}
			
			// Wait until too many entries were evicted from cache and then persist to disk to free some RAM
			if (!(_recentlyRemovedEntries.size() > PERSIST_THRESHOLD)){
				continue;
			}
			
			Map<String,LinkedList<String>> loadedMap = loadDataFromDisk();
			// Create a copy in order to avoid locking during persistence and cleaning
			HashMap<String,LinkedList<String>> removedEntriesCopy = new HashMap<>(_recentlyRemovedEntries);
			// Merge data from disk with new data and update duplicate keys
			Map<String,LinkedList<String>> mergedLoadedAndRecentlyRemovedMap = loadedMap != null ? mergeMaps(removedEntriesCopy, loadedMap) : _recentlyRemovedEntries;
			// Persist everything back to disk
			persistDataToDisk(mergedLoadedAndRecentlyRemovedMap);
			
			// Clean only persisted entries with the old value(could be possibly updated)
			for (Map.Entry<String, LinkedList<String>> entry : removedEntriesCopy.entrySet()){
				_recentlyRemovedEntries.remove(entry.getKey(),entry.getValue()); 
			}
		}
	}
	
	
	/**
	 * Before terminating the server simply persist all available data by waking the thread 
	 * */
	public void stopThreadAndPersistData() {
		// Check if the thread is sleeping(in our case he can not be waiting)
		if (getState().equals(State.TIMED_WAITING)){
			interrupt();
		}
	}
	
	/**
	 * Wait till the sleeping cycle will end and then gracefully terminate the thread. 
	 * */
	public void stopAfterNextCycle(){
		_running = false;
	}
}
