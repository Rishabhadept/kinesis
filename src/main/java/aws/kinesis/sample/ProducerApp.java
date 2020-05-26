package aws.kinesis.sample;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 *
 */
public class ProducerApp 
{

	static AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

	public static void main( String[] args ) throws JsonProcessingException
	{

		clientBuilder.setRegion("us-east-1");
		clientBuilder.setCredentials(new AWSStaticCredentialsProvider(new
				BasicAWSCredentials("<ACCESS_KEY>", "<ACCESS_SECRET>")));
		clientBuilder.setClientConfiguration(new ClientConfiguration().withUserAgentPrefix(ClientConfiguration.DEFAULT_USER_AGENT)
				.withUserAgentSuffix("kinesis-sample/1.0.0"));

		AmazonKinesis kinesisClient = clientBuilder.build();

		PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
		putRecordsRequest.setStreamName("test-stream");
		List <PutRecordsRequestEntry> putRecordsRequestEntryList  = buildEmployeeData();

		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
		System.out.println("Put Result" + putRecordsResult);


		//Failure Handling
		while (putRecordsResult.getFailedRecordCount() > 0) {
			final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
			final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
			for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
				final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
				final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
				if (putRecordsResultEntry.getErrorCode() != null) {
					failedRecordsList.add(putRecordRequestEntry);
				}
			}
			putRecordsRequestEntryList = failedRecordsList;
			putRecordsRequest.setRecords(putRecordsRequestEntryList);
			putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
		}
	}

	private static List<PutRecordsRequestEntry> buildEmployeeData() throws JsonProcessingException {

		ObjectMapper mapper = new ObjectMapper();

		List <PutRecordsRequestEntry> putRecordsRequestEntryList  =  new ArrayList<>();
		PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
		Employee employee1 = new Employee("Rishabh", "dev", 1);
		putRecordsRequestEntry.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee1))));
		putRecordsRequestEntry.setPartitionKey(employee1.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry2  = new PutRecordsRequestEntry();
		Employee employee2 = new Employee("Ryan", "qa", 2);
		putRecordsRequestEntry2.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee2))));
		putRecordsRequestEntry2.setPartitionKey(employee2.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry3  = new PutRecordsRequestEntry();
		Employee employee3 = new Employee("Rishi", "dev", 3);
		putRecordsRequestEntry3.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee3))));
		putRecordsRequestEntry3.setPartitionKey(employee3.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry4  = new PutRecordsRequestEntry();
		Employee employee4 = new Employee("sourav", "dev", 4);
		putRecordsRequestEntry4.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee4))));
		putRecordsRequestEntry4.setPartitionKey(employee4.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry5  = new PutRecordsRequestEntry();
		Employee employee5 = new Employee("sidhi", "qa", 5);
		putRecordsRequestEntry5.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee5))));
		putRecordsRequestEntry5.setPartitionKey(employee5.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry6  = new PutRecordsRequestEntry();
		Employee employee6 = new Employee("pawan", "dev", 6);
		putRecordsRequestEntry6.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee6))));
		putRecordsRequestEntry6.setPartitionKey(employee3.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry7  = new PutRecordsRequestEntry();
		Employee employee7 = new Employee("lalit", "dev", 7);
		putRecordsRequestEntry7.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee7))));
		putRecordsRequestEntry7.setPartitionKey(employee7.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry8  = new PutRecordsRequestEntry();
		Employee employee8 = new Employee("priya", "qa", 8);
		putRecordsRequestEntry8.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee8))));
		putRecordsRequestEntry8.setPartitionKey(employee8.getDesignation());
		
		PutRecordsRequestEntry putRecordsRequestEntry9  = new PutRecordsRequestEntry();
		Employee employee9 = new Employee("nishi", "dev", 9);
		putRecordsRequestEntry9.setData(ByteBuffer.wrap((mapper.writeValueAsBytes(employee9))));
		putRecordsRequestEntry9.setPartitionKey(employee9.getDesignation());
		
		putRecordsRequestEntryList.add(putRecordsRequestEntry); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry2); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry3); 
		
		putRecordsRequestEntryList.add(putRecordsRequestEntry4); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry5); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry6); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry7); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry8); 
		putRecordsRequestEntryList.add(putRecordsRequestEntry9); 
		
		return putRecordsRequestEntryList;
	}
}
