package upf.edu;

import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;

import java.util.List;

public class TwitterHashtagsReader {
    public static void main(String[] args) {
        String language = args[0];

        DynamoHashTagRepository hashTagRepository = new DynamoHashTagRepository();
        // Obtaining top10 most used hashtags for a given language
        List<HashTagCount> top10 = hashTagRepository.readTop10(language);
        // Printing the hashtags
        for(HashTagCount ht : top10){
            System.out.println(ht.toString());
        }
    }
}
