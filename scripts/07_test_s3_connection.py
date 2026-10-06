import boto3


BUCKET_NAME = "enterprise-operations-engineering-755642981182-eu-west-2-an"
REGION = "eu-west-2"


def main():
    s3 = boto3.client("s3", region_name=REGION)

    test_key = "test/connection_test.txt"
    test_content = "Enterprise Operations Engineering S3 connection successful."

    # Upload a small test object
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=test_key,
        Body=test_content,
    )

    print("Upload successful")
    print(f"Object: {test_key}")

    # Confirm that the object exists
    s3.head_object(
        Bucket=BUCKET_NAME,
        Key=test_key,
    )

    print("Object verification successful")

    # Remove the temporary object
    s3.delete_object(
        Bucket=BUCKET_NAME,
        Key=test_key,
    )

    print("Test object deleted")
    print("S3 connection test successful")


if __name__ == "__main__":
    main()