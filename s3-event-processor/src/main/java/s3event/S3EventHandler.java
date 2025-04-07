package s3event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class S3EventHandler implements RequestHandler<S3Event, String> {
    private final SnsClient snsClient = SnsClient.create();
    private final String topicArn = System.getenv("SNS_TOPIC_ARN");
    private final String environment = System.getenv("ENVIRONMENT");

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            context.getLogger().log("Received S3 event: " + s3Event);

            var bucket = s3Event.getRecords().getFirst().getS3().getBucket().getName();
            var key = s3Event.getRecords().getFirst().getS3().getObject().getKey();
            var eventTime = s3Event.getRecords().getFirst().getEventTime();
            var objectSize = s3Event.getRecords().getFirst().getS3().getObject().getSizeAsLong();

            var env = environment.equals("prod") ? "production" : "development";
            var subject = "New upload for s3 bucket " + bucket + " in " + env + " environment";
            var message = String.format(
                    """
                            A new file has been uploaded to your s3 bucket in %s environment.
                            
                            Details:
                            Bucket: %s
                            File: %s
                            Size: %s bytes
                            Upload Time: %s
                            """,
                    env, bucket, key, objectSize, eventTime
            );

            PublishResponse response = snsClient.publish(PublishRequest.builder()
                            .topicArn(topicArn)
                            .subject(subject)
                            .message(message)
                            .build()
            );

            context.getLogger().log("SNS notification sent with message ID: " + response.messageId());
            return "S3 event processed successfully";
        } catch (Exception e) {
            context.getLogger().log("Error processing S3 event: " + e.getMessage());
            throw new RuntimeException("Error processing S3 event", e);
        }
    }
}
