package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import upf.edu.model.HashTagCount;
import upf.edu.util.HashTagCountComparator;

import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

  final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
  final static String region = "us-east-1";
  final AmazonDynamoDB client;
  final DynamoDB dynamoDB;
  final Table dynamoDBTable;

  public DynamoHashTagRepository() {
    client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider())
            .build();
    dynamoDB = new DynamoDB(client);
    dynamoDBTable = dynamoDB.getTable("LSDS2020-TwitterHashtags");
  }

  @Override
  public void write(Status tweet) {
    // IMPLEMENT ME
    HashtagEntity[] hashtags = tweet.getHashtagEntities();
    String language = tweet.getLang();
    long tweetId = tweet.getId();
    for (HashtagEntity hashtag : hashtags) {
      try { // Trying to update element
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Hashtag", hashtag.getText(), "HTLanguage", language)
                // This will throw an exception if the hashtag does not exist as it has no tweetCount
                .withUpdateExpression("set tweetCount = tweetCount + :c, tweetIds = list_append(tweetIds, :id)")
                .withValueMap(new ValueMap()
                        .withNumber(":c", 1)
                        .withList(":id", Collections.singletonList(tweetId))
                );
        dynamoDBTable.updateItem(updateItemSpec);
      }
      catch (Exception ex) { // Element does not exist, we create it
        List<Long> myList = new ArrayList<>();
        myList.add(tweetId); // We add a 1 item List with the id
        dynamoDBTable.putItem(new Item()
                .withPrimaryKey("Hashtag", hashtag.getText(), "HTLanguage", language)
                .with("tweetCount", 1)
                .withList("tweetIds", myList)
        );
      }
    }
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
    // We define lng as the language to use it in the filterExpression
    Map<String, Object> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":lng", lang);

    // We scan the table and filter by language
    ItemCollection<ScanOutcome> items = dynamoDBTable.scan("HTLanguage = :lng",
            "Hashtag, HTLanguage, tweetCount, tweetIds",
            null,
            expressionAttributeValues);

    // For each item we create the corresponding HashTagCount Object and add it to the top10 list
    List<HashTagCount> top10 = new ArrayList<>();
    for (Item item : items) {
      top10.add(new HashTagCount(
              item.get("Hashtag").toString(),
              item.get("HTLanguage").toString(),
              item.getNumber("tweetCount").longValue()
      ));
    }
    // We sort descending
    top10.sort(Collections.reverseOrder(new HashTagCountComparator())); // sorting using comparator
    //top10.sort(Collections.reverseOrder()); // Sorting using comparable
    if(top10.size() > 10) {
      return top10.subList(0,10);
    }
    return top10;
  }

}
