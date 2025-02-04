import mongoose, { Schema, Document } from "mongoose";
interface IVideo extends Document {
  EpisodeNo: Number;
  animeId: mongoose.Schema.Types.ObjectId;
  episodeName: string;
  url: string;
  comments: mongoose.Schema.Types.ObjectId[];
}

const videoScheme: Schema = new mongoose.Schema({
  EpisodeNo: { type: Number, required: true },
  animeId: { type: mongoose.Schema.Types.ObjectId, ref: "Anime", required: true },
  episodeName: { type: String, required: true },
  url: { type: String, required: true },
  comments: [
    { type: mongoose.Schema.Types.ObjectId, ref: "Comment", required: true },
  ],
});

const AnimeVideo = mongoose.model<IVideo>("AnimeVideo", videoScheme);

export default AnimeVideo;
