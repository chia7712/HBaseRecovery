package net.spright.salter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.spright.shadow.STableName;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class Salter {
    public static abstract class Encoder {
        private final Map<byte[], Put> putMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        abstract public void encode(KeyValue keyValue);
        public void encode(List<KeyValue> keyValueList) {
            for(KeyValue keyValue : keyValueList) {
                encode(keyValue);
            }
        }
        public List<Put> take() {
            try {
                return new ArrayList(putMap.values());
            } finally {
                putMap.clear();
            }
        }
        public void clear() {
            putMap.clear();
        }
        public int size() {
            return putMap.size();
        }
        protected Put getPut(byte[] saltingRow) {
            Put put = putMap.get(saltingRow);
            if(put == null) {
                put = new Put(saltingRow);
                putMap.put(saltingRow, put);
            }
            return put;
        }
    }
    public static abstract class Decoder {
        private final Map<byte[], List<KeyValue>> familyMap = new TreeMap(Bytes.BYTES_RAWCOMPARATOR);
        private final Map<byte[], Long> familySize = new TreeMap(Bytes.BYTES_RAWCOMPARATOR);
        private final Set<byte[]> familiesOverHeap = new TreeSet(Bytes.BYTES_RAWCOMPARATOR);
        private	final long maxHeapSize;
        public Decoder(long maxHeapSize) {
            this.maxHeapSize = maxHeapSize <= 0 ? Long.MAX_VALUE : maxHeapSize;
        }
        public KeyValue decodeWithoutBuffer(KeyValue saltingKeyValue) {
            return internalDecode(saltingKeyValue);
        }
        private List<KeyValue> getKeyValueList(byte[] family) {
            List<KeyValue> keyValueList = familyMap.get(family);
            if(keyValueList == null) {
                keyValueList = new LinkedList();
                familyMap.put(family, keyValueList);
            }
            return keyValueList;
        }
        private void checkSize(byte[] family, long size) {
            Long currentSize = familySize.get(family);
            if(currentSize == null) {
                familySize.put(family, size);
            }
            else {
                final long heapSize = currentSize + size;
                if(heapSize >= maxHeapSize) {
                    familiesOverHeap.add(family);
                }
                familySize.put(family, heapSize);
            }
        }
        private static long getSize(KeyValue keyValue) {
            return keyValue.getLength();
        }
        public KeyValue decode(KeyValue saltingKeyValue) {
            KeyValue decodedKeyValue = internalDecode(saltingKeyValue);
            if(decodedKeyValue == null){
                return null;
            }
            byte[] family = decodedKeyValue.getFamily();
            List<KeyValue> keyValueList = getKeyValueList(family);
            keyValueList.add(decodedKeyValue);
            checkSize(family, getSize(decodedKeyValue));
            return decodedKeyValue;
        }
        public long decode(List<KeyValue> saltingKeyValueList) {
            long count = 0;
            for(KeyValue saltingKeyValue : saltingKeyValueList) {
                count = decode(saltingKeyValue) == null ? count : count + 1;
            }
            return count;
        }
        public List<List<KeyValue>> take() {
            try {
                List<List<KeyValue>> keyValueSetList = new LinkedList();
                for(Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
                        keyValueSetList.add(entry.getValue());
                }
                return keyValueSetList;
            }
            finally {
                clear();
            }
        }
        public List<KeyValue> take(byte[] family) {
            try {
                List<KeyValue> keyValueSet = familyMap.get(family);
                if(keyValueSet != null) {
                    return keyValueSet;
                }
                return new LinkedList();
            } finally {
                familyMap.remove(family);
                familySize.remove(family);
                familiesOverHeap.remove(family);
            }
        }
        public Set<byte[]> overHeapSize() {
            return new HashSet(familiesOverHeap);
        }
        public int overHeapSizeNumber() {
            return familiesOverHeap.size();
        }
        public long size() {
            return familyMap.size();
        }
        public void clear() {
            familyMap.clear();
            familySize.clear();
            familiesOverHeap.clear();
        }
        abstract protected KeyValue internalDecode(KeyValue saltingKeyValue);
}
    public static abstract class Scanner {
        protected final STableName stableName;
        protected final TimeInterval timeInterval;
        protected final List<byte[]> families;
        public Scanner(STableName stableName) {
            this(stableName, null, null);
        }
        public Scanner(STableName stableName, TimeInterval timeInterval) {
            this(stableName, timeInterval, null);
        }
        public Scanner(STableName stableName, TimeInterval timeInterval, List<byte[]> families) {
            this.stableName = stableName.copyOf();
            if(timeInterval == null) {
                timeInterval = new TimeInterval();
            }
            this.timeInterval = timeInterval;
            this.families = new LinkedList();
            if(families != null) {
                this.families.addAll(families);
            }
        }
        public abstract Scan createScan(HRegionInfo regionInfo);
        protected abstract Scan[] internalCreatScans(int number);
        public Scan[] createScans(int number) {
            return internalCreatScans(number);
        }
        public Scan[] createScans(int number, int caching) {
            Scan[] scans = internalCreatScans(number);
            for(Scan scan : scans) {
                scan.setCaching(caching);
                scan.setBatch(caching);
            }
            return scans;
        }
        public STableName createSTableName() {
            return stableName.copyOf();
        }
        public TimeInterval createTimeInterval() {
            return timeInterval.copyOf();
        }
    }
    private Salter(){}
}
