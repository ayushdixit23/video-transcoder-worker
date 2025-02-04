import {
  S3Client,
} from "@aws-sdk/client-s3";
import {
  AWS_ACCESS_KEY_ID,
  AWS_REGION,
  AWS_SECRET_ACCESS_KEY,
} from "./envConfig.js";

interface S3ClientConfig {
  region: string;
  credentials: {
    accessKeyId: string;
    secretAccessKey: string;
  };
}

const s3ClientConfig: S3ClientConfig = {
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
};

const s3Client = new S3Client(s3ClientConfig);


export { s3Client };
