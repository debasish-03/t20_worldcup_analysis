from google.cloud import storage



def upload_to_gcs(bucket_name, local_file_name, destination_object_name):
        print('bucket_name: ', bucket_name)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name=bucket_name)

        blob = bucket.blob(destination_object_name)
        blob.upload_from_filename(local_file_name)

        print(
            f"File {local_file_name} uploaded to {destination_object_name}."
        )