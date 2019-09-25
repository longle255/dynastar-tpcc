package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastarv2.Partition;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;

public class Util {
    public static int mapIdToPartition(ObjId objId) {
        return objId.hashCode() % Partition.getPartitionList().size();
    }
}
