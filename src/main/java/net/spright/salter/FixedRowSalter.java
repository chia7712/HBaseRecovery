package net.spright.salter;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import net.spright.id.HdfsUuid;
import net.spright.shadow.STableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import static org.apache.hadoop.hbase.KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
import static org.apache.hadoop.hbase.KeyValue.KEY_INFRASTRUCTURE_SIZE;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class FixedRowSalter {
    public static final int PREFIX_OFFSET = 0;
    public static final int PREFIX_LENGTH = Integer.SIZE / 8;
    public static final int BATCH_ID_OFFSET = PREFIX_OFFSET + PREFIX_LENGTH;
    public static final int BATCH_ID_LENGTH = Long.SIZE / 8;
    public static final int REGION_ID_OFFSET = BATCH_ID_OFFSET + BATCH_ID_LENGTH;
    public static final int REGION_ID_LENGTH = Integer.SIZE / 8;
    public static final int KEYVALUE_ID_OFFSET = REGION_ID_OFFSET + REGION_ID_LENGTH;
    public static final int KEYVALUE_ID_LENGTH = Integer.SIZE / 8;
    public static final int UUID_OFFSET = PREFIX_OFFSET + PREFIX_LENGTH;
    public static final int UUID_LENGTH = BATCH_ID_LENGTH + REGION_ID_LENGTH + KEYVALUE_ID_LENGTH;
    public static final int TYPE_OFFSET = KEYVALUE_ID_OFFSET + KEYVALUE_ID_LENGTH;
    public static final int TYPE_LENGTH = Byte.SIZE / 8;
    public static final int ROW_LENGTH_OFFSET = TYPE_OFFSET + TYPE_LENGTH;
    public static final int ROW_LENGTH_LENGTH = Integer.SIZE / 8;
    public static final int SALTING_ROW_SIZE = PREFIX_LENGTH + UUID_LENGTH + TYPE_LENGTH + ROW_LENGTH_LENGTH;
    public static Salter.Decoder createDecoder() {
        return new DecoderImpl();	
    }
    public static Salter.Decoder createDecoder(long maxHeapSize) {
        return new DecoderImpl(maxHeapSize);	
    }
    public static Salter.Encoder createEncoder(Prefixer prefixer, HdfsUuid.Builder uuidBuilder) {
        return new EncoderImpl(prefixer, uuidBuilder);
    }
    public static Salter.Scanner createScanner(STableName stableName, TimeInterval timeInterval, List<byte[]> families) {
        return new ScanImpl(stableName, timeInterval, families);
    }
    private static class DecoderImpl extends Salter.Decoder {
        public DecoderImpl() {
            super(-1);
        }
        public DecoderImpl(long maxHeapSize) {
            super(maxHeapSize);
        }
        @Override
        protected KeyValue internalDecode(KeyValue saltingKeyValue) {
            //check the length of slating row
            int saltingRowLength = saltingKeyValue.getRowLength();
            if(saltingRowLength != (SALTING_ROW_SIZE))
                    return null;

            int saltingRowOffset = saltingKeyValue.getRowOffset();
            byte[] saltingBuffer = saltingKeyValue.getBuffer();
            //get the length of original row 
            final int rlength = Bytes.toInt(saltingBuffer, saltingRowOffset + ROW_LENGTH_OFFSET, ROW_LENGTH_LENGTH);
            //get the length of salting value
            int saltingValueLength = saltingKeyValue.getValueLength();
            final int vlength =  saltingValueLength - rlength;
            //if the length of salting row is less than length of original row, the salting keyvalue has invalid format.
            if(vlength <= 0)
                    return null;
            //format check is over
            byte type = saltingBuffer[saltingRowOffset + TYPE_OFFSET];
            final int flength = saltingKeyValue.getFamilyLength();
            final int qlength = saltingKeyValue.getQualifierLength();
            final int keylength = KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
            byte[] bytes = new byte[KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength];
            
            int pos = 0;
            //step.A add length of key(row length, row, family length, family, qualifier, timestamp, type)
            pos = Bytes.putInt(bytes, pos, keylength);
            //step.B add length of value
            pos = Bytes.putInt(bytes, pos, vlength);
            //step.C add length of row
            pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
            //step.D [add row]
            pos = Bytes.putBytes(bytes, pos, saltingBuffer, saltingKeyValue.getValueOffset(), rlength);
            //step.E add length of family
            pos = Bytes.putByte(bytes, pos, (byte)(flength & 0x0000ff));
            //step.F add family
            if(flength != 0)
            {
                pos = Bytes.putBytes(bytes, pos, saltingBuffer, saltingKeyValue.getFamilyOffset(), flength);
            }
            //step.G add qualifier
            if(qlength != 0) 
            {
                pos = Bytes.putBytes(bytes, pos, saltingBuffer, saltingKeyValue.getQualifierOffset(), qlength);
            }
            //step.H add timestamp
            pos = Bytes.putLong(bytes, pos, saltingKeyValue.getTimestamp());
            //step.I add type
            pos = Bytes.putByte(bytes, pos, type);
            //step.J add value
            Bytes.putBytes(bytes, pos, saltingBuffer, saltingKeyValue.getValueOffset() + rlength,  vlength);

            return new KeyValue(bytes, 0, bytes.length);
        }
    }
    private static class EncoderImpl extends Salter.Encoder {
        private final Prefixer prefixer;
        private final HdfsUuid.Builder uuidBuilder;
        public EncoderImpl(Prefixer prefixer, HdfsUuid.Builder uuidBuilder) {
            this.prefixer = prefixer;
            this.uuidBuilder = uuidBuilder;
        }
        @Override
        public void encode(KeyValue keyValue) {
            final int prefix = prefixer.createPrefix();
            final byte[] oriBuffer = keyValue.getBuffer();
            final int rlength = SALTING_ROW_SIZE;
            final int flength = keyValue.getFamilyLength();
            final int qlength = keyValue.getQualifierLength();
            final int vlength = keyValue.getRowLength() + keyValue.getValueLength();
            final int keylength = KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
            byte[] bytes = new byte[KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength];
            int pos = 0;
            //step.A add length of key(row length, row, family length, family, qualifier, timestamp, type)
            pos = Bytes.putInt(bytes, pos, keylength);
            //step.B add length of value
            pos = Bytes.putInt(bytes, pos, vlength);
            //step.C add length of row
            pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
            //step.D [add salting row]
            byte[] saltingRow = createSaltingRow(keyValue, prefix, uuidBuilder);
            pos = Bytes.putBytes(bytes, pos, saltingRow, 0, saltingRow.length);
            //step.E add length of family
            pos = Bytes.putByte(bytes, pos, (byte)(flength & 0x0000ff));
            //step.F add family
            if(flength != 0)
            {
                pos = Bytes.putBytes(bytes, pos, oriBuffer, keyValue.getFamilyOffset(), flength);
            }
            //step.G add qualifier
            if(qlength != 0) 
            {
                pos = Bytes.putBytes(bytes, pos, oriBuffer, keyValue.getQualifierOffset(), qlength);
            }
            //step.H add timestamp
            pos = Bytes.putLong(bytes, pos, keyValue.getTimestamp());
            //step.I add type
            pos = Bytes.putByte(bytes, pos, KeyValue.Type.Put.getCode());
            //step.J add value
            //step.J.1 add original row
            pos = Bytes.putBytes(bytes, pos, oriBuffer, keyValue.getRowOffset(), keyValue.getRowLength());
            //step.J.2 add original value
            Bytes.putBytes(bytes, pos, oriBuffer, keyValue.getValueOffset(), keyValue.getValueLength());
            
            Put put = getPut(saltingRow);
            try
            {
                put.add(new KeyValue(bytes, 0, bytes.length));
            }
            catch (IOException ex){}
        }
        private static byte[] createSaltingRow(KeyValue keyValue, int prefix, HdfsUuid.Builder uuidBuilder) {
            byte[] saltingRow = new byte[SALTING_ROW_SIZE];
            long batchID = uuidBuilder.getbatchID();
            int regionID = uuidBuilder.getRegionID();
            int keyvaluID = uuidBuilder.createtKeyValueID();
            int pos = 0;
            //step.1 add prefeix row, 4 byte, index of 0~3
            pos = Bytes.putInt(saltingRow, pos, prefix);
            //step.2 add UUID, 16 bytes, index of 4~19
            pos = Bytes.putLong(saltingRow, pos, batchID);
            pos = Bytes.putInt(saltingRow, pos, regionID);
            pos = Bytes.putInt(saltingRow, pos, keyvaluID);
            //step.3 add KeyValue type, 1 byte, index of 20
            pos = Bytes.putByte(saltingRow, pos, keyValue.getType());
            //step.4 add row length, 4 bytes, index of 21 ~24
            Bytes.putInt(saltingRow, pos, keyValue.getRowLength());
            return saltingRow;
        }
    }
    private static class ScanImpl extends Salter.Scanner {
        public ScanImpl(STableName stableName, TimeInterval timeInterval, List<byte[]> families) {
            super(stableName, timeInterval, families);
        }
        @Override
        public Scan createScan(HRegionInfo regionInfo) {
            byte[] row = regionInfo.getStartKey();
            if(Bytes.compareTo(row, HConstants.EMPTY_START_ROW) == 0)
                row = Bytes.toBytes((int)0);
            byte[] prefix = new byte[PREFIX_LENGTH];
            System.arraycopy(row, 0, prefix, 0, PREFIX_LENGTH);
            return createScan(Bytes.toInt(prefix));
        }
        @Override
        public Scan[] internalCreatScans(int number) {
            List<Scan> scanList = new LinkedList();
            for(int index = 0; index != number; ++index) {
                scanList.add(createScan(index));
            }
            return scanList.toArray(new Scan[scanList.size()]);
        }
        @Override
        public TimeInterval createTimeInterval() {
            return timeInterval.copyOf();
        }
        private Scan createScan(int prefix) {
            byte[] startRow = new byte[SALTING_ROW_SIZE];
            byte[] stopRow = new byte[SALTING_ROW_SIZE];
            if(timeInterval.getminTimestamp() != TimeInterval.MIN_TIME) {
                System.arraycopy(
                    Bytes.toBytes(timeInterval.getminTimestamp()), 
                    0, 
                    startRow, 
                    BATCH_ID_OFFSET, 
                    BATCH_ID_LENGTH);
            }
            System.arraycopy(
                Bytes.toBytes(prefix), 
                0, 
                startRow, 
                PREFIX_OFFSET, 
                PREFIX_LENGTH);
            if(timeInterval.getmaxTimestamp() == TimeInterval.MAX_TIME) {
                System.arraycopy(
                    Bytes.toBytes(prefix + 1), 
                    0, 
                    stopRow, 
                    PREFIX_OFFSET, 
                    PREFIX_LENGTH);
            } else {
                Arrays.fill(stopRow, (byte)0xff);
                System.arraycopy(
                    Bytes.toBytes(timeInterval.getmaxTimestamp()), 
                    0, 
                    stopRow, 
                    BATCH_ID_OFFSET, 
                    BATCH_ID_LENGTH);
                System.arraycopy(
                    Bytes.toBytes(prefix), 
                    0, 
                    stopRow, 
                    PREFIX_OFFSET, 
                    PREFIX_LENGTH);
            }
            Scan scan = new Scan(startRow, stopRow);
            for(byte[] family : families) {
                scan.addFamily(family);
            }
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, stableName.toBytes());
            return scan;
        }
    }
    private FixedRowSalter(){}
}
