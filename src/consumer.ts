import { DeleteObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import amqp from 'amqplib';
import { s3Client } from './helpers/s3.js';
import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';
import { Upload } from "@aws-sdk/lib-storage";

const CONCURRENT_JOBS = 1;
const TEMP_DIR = '/tmp/video_transcoder'; // Change to your temp storage location

const mkdir = promisify(fs.mkdir);
const readdir = promisify(fs.readdir);
const rm = promisify(fs.rm);

async function ensureDirectoryExists(dirPath: string) {
    try {
        await mkdir(dirPath, { recursive: true });
        console.log(`Created temp directory: ${dirPath}`);
    } catch (error: any) {
        if (error.code !== 'EEXIST') {
            console.error(`Error creating directory ${dirPath}:`, error);
            throw error;
        }
    }
}

async function consumeMessages(): Promise<void> {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queue = 'video_transcoding';

    await channel.assertQueue(queue, { durable: true });
    await channel.prefetch(CONCURRENT_JOBS);

    console.log('Waiting for messages...');
    channel.consume(queue, handleMessage(channel), { noAck: false });
}

const handleMessage = (channel: amqp.Channel) => async (msg: amqp.Message | null): Promise<void> => {
    if (!msg) {
        console.warn('Received null message');
        return;
    }

    const message = JSON.parse(msg.content.toString()) as { bucket: string; filename: string };
    const { bucket, filename } = message;

    // Extract folder name from filename
    const folderName = path.dirname(filename);
    const tempPath = path.join(TEMP_DIR, folderName);

    try {
        // Ensure directory is created once here
        await ensureDirectoryExists(tempPath);
        console.log(`Created temp directory: ${tempPath}`);

        console.log(`Received transcoding request: ${filename} from ${bucket}`);
        await processVideoFromS3(bucket, filename, tempPath);
        console.log(`Video processing completed: ${filename}`);

        await deleteOriginalVideoFromS3(bucket, filename);

        channel.ack(msg);
    } catch (err: any) {
        console.error(`Failed to process message: ${err.message}`);
        // channel.nack(msg, false, true); // Requeue the message
        channel.ack(msg);
    }
};

async function deleteOriginalVideoFromS3(bucket: string, filename: string): Promise<void> {
    console.log(`Deleting ${filename} from S3...`);

    try {
        await s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: filename }));
        console.log(`Successfully deleted ${filename} from S3.`);
    } catch (error: any) {
        console.error(`Failed to delete ${filename} from S3:`, error.message);
    }
}

async function processVideoFromS3(bucket: string, filename: string, tempPath: string): Promise<void> {
    const localFilePath = path.join(tempPath, path.basename(filename));

    console.log(`Downloading ${filename} from S3...`);
    await downloadFileFromS3(bucket, filename, localFilePath);

    console.log(`Transcoding ${filename} to HLS...`);
    await transcodeToHLS(localFilePath, bucket, filename);

    console.log(`Cleaning up local files...`);
    await cleanupTempFiles(localFilePath);
}

async function downloadFileFromS3(bucket: string, key: string, localPath: string): Promise<void> {
    const data = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    return new Promise((resolve, reject) => {
        const fileStream = fs.createWriteStream(localPath);
        (data.Body as NodeJS.ReadableStream).pipe(fileStream);
        fileStream.on('finish', resolve);
        fileStream.on('error', reject);
    });
}

async function transcodeToHLS(inputFile: string, outputBucket: string, outputKey: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const outputBase = outputKey.split('.')[0]; // Remove file extension
        const outputFolder = path.join(TEMP_DIR, outputBase);
        fs.mkdirSync(outputFolder, { recursive: true });

        const hlsVariants = [
            { name: '360p', resolution: '640x360', bitrate: '500k' },
            { name: '480p', resolution: '854x480', bitrate: '1000k' },
            { name: '720p', resolution: '1280x720', bitrate: '2500k' },
            { name: '1080p', resolution: '1920x1080', bitrate: '5000k' },
        ];

        let masterPlaylistContent = '#EXTM3U\n#EXT-X-VERSION:3\n';

        const ffmpegCommand = ffmpeg(inputFile)
            .outputOptions([
                '-preset veryfast',
                '-g 48',
                '-sc_threshold 0',
                '-hls_time 10',
                '-hls_playlist_type vod',
            ]);

        hlsVariants.forEach((variant) => {
            const variantPlaylist = `${variant.name}.m3u8`;
            ffmpegCommand
                .output(path.join(outputFolder, variantPlaylist))
                .videoCodec('libx264')
                .audioCodec('aac')
                .outputOptions([
                    `-s ${variant.resolution}`,
                    `-b:v ${variant.bitrate}`,
                    `-hls_segment_filename ${outputFolder}/${variant.name}_%03d.ts`
                ]);

            masterPlaylistContent += `#EXT-X-STREAM-INF:BANDWIDTH=${parseInt(variant.bitrate) * 1000},RESOLUTION=${variant.resolution}\n`;
            masterPlaylistContent += `${variantPlaylist}\n`;
        });

        ffmpegCommand
            .on('end', async () => {
                console.log('HLS processing completed. Writing master playlist...');

                const masterPlaylistPath = path.join(outputFolder, 'index.m3u8');
                fs.writeFileSync(masterPlaylistPath, masterPlaylistContent);

                console.log('Uploading HLS files to S3...');
                await uploadHLSFilesToS3(outputFolder, outputBucket, outputBase);
                resolve();
            })
            .on('error', (err) => {
                console.error('HLS Processing Error:', err);
                reject(err);
            })
            .run();
    });
}

async function uploadHLSFilesToS3(localFolder: string, bucket: string, s3Folder: string): Promise<void> {
    const files = await readdir(localFolder);
    const uploadPromises = files.map(async (file) => {
        const filePath = path.join(localFolder, file);
        const s3Key = `${s3Folder}/${file}`;

        await uploadFileToS3(filePath, bucket, s3Key);
    });

    await Promise.all(uploadPromises);
    console.log('All HLS files uploaded successfully.');
}


async function uploadFileToS3(filePath: string, bucket: string, s3Key: string): Promise<void> {
    try {
        const fileStream = fs.createReadStream(filePath);

        const upload = new Upload({
            client: s3Client,
            params: { Bucket: bucket, Key: s3Key, Body: fileStream },
        });

        await upload.done();
        console.log(`Uploaded: ${s3Key}`);
    } catch (error) {
        console.error('Error uploading file:', error);
    }
}

async function cleanupTempFiles(filePath: string) {
    try {
        // Get the 'death-note' folder from the file path
        const parentFolder = path.dirname(filePath); // Gets '/tmp/video_transcoder/death-note'

        await rm(parentFolder, { recursive: true, force: true });

        console.log(`Successfully deleted folder: ${parentFolder}`);
    } catch (error: any) {
        console.warn(`Cleanup failed: ${error.message}`);
    }
}


consumeMessages().catch(console.error);
