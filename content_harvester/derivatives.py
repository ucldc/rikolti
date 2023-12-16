import os
import subprocess

from .settings import CONTENT_PROCESSES


class ThumbnailError(Exception):
    pass


# decorator function
def subprocess_exception_handler(func):
    def wrapper(*args, **kwargs):
        # try:
        return func(*args, **kwargs)
        # except subprocess.CalledProcessError as e:
        #     print(
        #         f"{func.__name__} command failed: {e.cmd}\n"
        #         f"returncode was: {e.returncode}\n"
        #         f"output was: {e.output}"
        #     )
        #     raise(e)
        #     # return None
    return wrapper


@subprocess_exception_handler
def pdf_to_thumb(pdf_file_path):
    '''
        generate thumbnail image for PDF
        use ImageMagick `convert` tool as described here:
        http://www.nuxeo.com/blog/qa-friday-thumbnails-pdf-psd-documents/
    '''
    thumb_file_path = f"{pdf_file_path.split('.')[0]}.png"
    if os.path.exists(thumb_file_path):
        return thumb_file_path

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-quiet", "-strip", "-format", "png", "-quality",
        "75", f"{pdf_file_path}[0]", thumb_file_path
    ]
    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(f"Used ImageMagic `convert` {pdf_file_path} to {thumb_file_path}")
    return thumb_file_path


@subprocess_exception_handler
def video_to_thumb(video_path):
    '''
        generate thumbnail image for video
        use ffmpeg to grab center frame from video
    '''
    thumb_path = f"{video_path.split('.')[0]}.png"
    if os.path.exists(thumb_path):
        return thumb_path

    ffprobe_location = CONTENT_PROCESSES['ffprobe']
    duration_proc = [
        ffprobe_location, '-v', 'fatal', '-show_entries', 'format=duration',
        '-of', 'default=nw=1:nk=1', video_path
    ]

    # get duration of video
    duration = subprocess.check_output(duration_proc, stderr=subprocess.STDOUT)

    # calculate midpoint of video
    midpoint = float(duration.strip()) / 2
    ffmpeg_location = CONTENT_PROCESSES['ffmpeg']

    process = [
        ffmpeg_location, '-v', 'fatal', '-ss', str(midpoint), '-i', video_path,
        '-vframes', '1', thumb_path
    ]

    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(f"Used ffmpeg to convert {video_path} to {thumb_path}")
    return thumb_path


@subprocess_exception_handler
def make_jp2(source_file_path):
    ''' convert a tiff to jp2 using image magick.'''
    jp2_path = f"{source_file_path.split('.')[0]}.jp2"
    if os.path.exists(jp2_path):
        return jp2_path

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-quiet", "-format", "-jp2", "-define",
        "jp2:rate=10", f"{source_file_path}[0]", jp2_path
    ]
    msg = f"{source_file_path} converted to {jp2_path}"

    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(msg)
    return jp2_path
