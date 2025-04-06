package s3event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class S3EventHandler implements RequestHandler<S3Event, String> {
    private final AmazonSNS snsClient = AmazonSNSClientBuilder.defaultClient();
    private final String topicArn = System.getenv("SNS_TOPIC_ARN");

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            context.getLogger().log("Received S3 event: " + s3Event);

            var bucket = s3Event.getRecords().getFirst().getS3().getBucket().getName();
            var key = s3Event.getRecords().getFirst().getS3().getObject().getKey();
            var eventTime = s3Event.getRecords().getFirst().getEventTime();
            var objectSize = s3Event.getRecords().getFirst().getS3().getObject().getSizeAsLong();

            var subject = "New upload for s3 bucket";
            var message = String.format(
                    """
                            A new file has been uploaded to your s3 bucket.
                            
                            Details:
                            Bucket: %s
                            File: %s
                            Size: %s bytes
                            Upload Time: %s
                            """,
                    bucket, key, objectSize, eventTime
            );

            PublishResult result = snsClient.publish(new PublishRequest()
                    .withTopicArn(topicArn)
                    .withSubject(subject)
                    .withMessage(message)
            );

            context.getLogger().log("SNS notification sent with message ID: " + result.getMessageId());
            return "S3 event processed successfully";
        } catch (Exception e) {
            context.getLogger().log("Error processing S3 event: " + e.getMessage());
            throw new RuntimeException("Error processing S3 event", e);
        }
    }
}
