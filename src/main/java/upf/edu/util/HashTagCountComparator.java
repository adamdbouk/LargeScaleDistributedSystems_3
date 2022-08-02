package upf.edu.util;

import upf.edu.model.HashTagCount;

import java.util.Comparator;

public class HashTagCountComparator implements Comparator<HashTagCount> {
    // Comparator class so we can sort a HashTagCount collection by the Hashtag counter
    @Override
    public int compare(HashTagCount hTCount1, HashTagCount hTCount2) {
        return Long.compare(hTCount1.getCount(), hTCount2.getCount());
    }
}
