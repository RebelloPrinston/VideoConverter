import pika, json, tempfile, os
from bson.objectid import ObjectId
import moviepy.editor
from contextlib import contextmanager

def start(message, fs_videos, fs_mp3s, channel):
    try:
        message = json.loads(message)
        
        with tempfile.NamedTemporaryFile(suffix='.mp4') as tf:
            # video contents
            out = fs_videos.get(ObjectId(message["video_fid"]))
            tf.write(out.read())
            tf.flush()

            # create audio from temp video file
            video = moviepy.editor.VideoFileClip(tf.name)
            audio = video.audio
            
            # write audio to file
            tf_path = tempfile.gettempdir() + f"/{message['video_fid']}.mp3"
            try:
                audio.write_audiofile(tf_path)
            finally:
                # Clean up moviepy resources
                audio.close()
                video.close()

            # save file to mongo
            with open(tf_path, "rb") as f:
                data = f.read()
                fid = fs_mp3s.put(data)

            # Clean up temp mp3 file
            os.remove(tf_path)

            message["mp3_fid"] = str(fid)

            # Publish to queue
            channel.basic_publish(
                exchange="",
                routing_key=os.environ.get("MP3_QUEUE"),
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )
            return None

    except Exception as err:
        print(f"Error processing video: {str(err)}")
        if 'fid' in locals():
            fs_mp3s.delete(fid)
        if 'tf_path' in locals() and os.path.exists(tf_path):
            os.remove(tf_path)
        return f"Error processing video: {str(err)}"