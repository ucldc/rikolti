import os
import subprocess


class UnsupportedMimetype(Exception):
    pass


class ThumbnailError(Exception):
    pass


def make_thumb_from_pdf(pdf_file_path, thumb_file_path):
    '''
        generate thumbnail image for PDF
        use ImageMagick `convert` tool as described here:
        http://www.nuxeo.com/blog/qa-friday-thumbnails-pdf-psd-documents/
    '''
    if os.path.exists(thumb_file_path):
        print(f"File already exists, won't overwrite: {thumb_file_path}")
        return True
    try:
        subprocess.check_output(
            [
                os.environ.get(
                    'PATH_MAGICK_CONVERT',
                    '/usr/local/bin/convert'
                ),
                "-quiet",
                "-strip",
                "-format",
                "png",
                "-quality",
                "75",
                pdf_file_path,
                thumb_file_path
            ],
            stderr=subprocess.STDOUT
        )
        print(
            "Used ImageMagic `convert` to convert "
            f"{pdf_file_path} to {thumb_file_path}"
        )
        return True
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ImageMagic `convert` command failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def make_thumb_from_video(video_path, thumb_path):
    '''
        generate thumbnail image for video
        use ffmpeg to grab center frame from video
    '''
    # get duration of video
    if os.path.exists(thumb_path):
        print(f"File already exists, won't overwrite: {thumb_path}")
        return True
    try:
        duration = subprocess.check_output(
            [
                os.environ.get(
                    'PATH_FFPROBE',
                    '/usr/local/bin/ffprobe'
                ),
                '-v',
                'fatal',
                '-show_entries',
                'format=duration',
                '-of',
                'default=nw=1:nk=1',
                video_path
            ],
            stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ffprobe check failed: {e.cmd}\n"
            "returncode was: {e.returncode}\n"
            "output was: {e.output}"
        )
        return False

    # calculate midpoint of video
    midpoint = float(duration.strip()) / 2

    try:
        subprocess.check_output(
            [
                os.environ.get(
                    'PATH_FFMPEG',
                    '/usr/local/bin/ffmpeg'
                ),
                '-v',
                'fatal',
                '-ss',
                str(midpoint),
                '-i',
                video_path,
                '-vframes',
                '1',  # output 1 frame
                thumb_path
            ],
            stderr=subprocess.STDOUT
        )
        print("Used ffmpeg to convert {video_path} to {thumb_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ffmpeg thumbnail creation failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def make_thumbnail(source_file_path, mimetype):
    '''
        generate thumbnail image for PDF, video, or image
    '''
    thumbnail_path = None
    success = False

    if mimetype not in ['image/jpeg', 'application/pdf', 'video/mp4']:
        raise UnsupportedMimetype(f"thumbnail: {mimetype}")

    if mimetype == 'image/jpeg':
        success = True
        thumbnail_path = source_file_path

    thumb_name = f"{source_file_path.split('.')[0]}.png"
    thumbnail_path = os.path.join('/tmp', thumb_name)

    if mimetype == 'application/pdf':
        success = make_thumb_from_pdf(
            f"{source_file_path}[0]", thumbnail_path)

    if mimetype == 'video/mp4':
        success = make_thumb_from_video(
            source_file_path, thumbnail_path)

    if not success:
        raise ThumbnailError(
            f"ERROR: creating a thumbnail: "
            f"{source_file_path}, mimetype: {mimetype}"
        )
    return thumbnail_path


def make_jp2(source_file_path):
    print(f"make jp2: {source_file_path}")
    return source_file_path


# def make_jp2():
#     convert = Convert()
#     passed = convert._pre_check(source['mimetype'])
#     if not passed:
#         return
#     jp2_report = {}

#     magick_tiff_filepath = os.path.join(tmp_dir, 'magicked.tif')
#     uncompressed_tiff_filepath = os.path.join(tmp_dir, 'uncompressed.tif')
#     prepped_filepath = os.path.join(tmp_dir, 'prepped.tiff')

#     # prep file for conversion to jp2
#     if source['mimetype'] in ['image/jpeg', 'image/gif', 'image/png']:
#         preconverted, preconvert_msg = convert._pre_convert(
#             tmp_filepath, magick_tiff_filepath)
#         jp2_report['pre_convert'] = {
#             'preconverted': preconverted,
#             'msg': preconvert_msg
#         }

#         tiff_to_srgb, tiff_to_srgb_msg = convert._tiff_to_srgb_libtiff(
#             magick_tiff_filepath, prepped_filepath)
#         jp2_report['tiff_to_srgb'] = {
#             'tiff_to_srgb': tiff_to_srgb,
#             'msg': tiff_to_srgb_msg
#         }

#     elif source['mimetype'] == 'image/tiff':
#         uncompressed, uncompress_msg = convert._pre_convert(
#             tmp_filepath, uncompressed_tiff_filepath)
#         jp2_report['uncompress_tiff'] = {
#             'uncompressed': uncompressed,
#             'msg': uncompress_msg
#         }

#         tiff_to_srgb, tiff_to_srgb_msg = convert._tiff_to_srgb_libtiff(
#             uncompressed_tiff_filepath, prepped_filepath)
#         jp2_report['tiff_to_srgb'] = {
#             'tiff_to_srgb': tiff_to_srgb,
#             'msg': tiff_to_srgb_msg
#         }

#     elif source['mimetype'] in ('image/jp2', 'image/jpx', 'image/jpm'):
#         uncompressed, uncompress_msg = convert._uncompress_jp2000(
#             tmp_filepath, prepped_filepath)
#         jp2_report['uncompress_jp2000'] = {
#             'uncompressed': uncompressed,
#             'msg': uncompress_msg
#         }

#     else:
#         msg = "Did not know how to prep file with mimetype {} for " \
#                 "conversion to jp2.".format(source['mimetype'])
#         jp2_report['status'] = 'unknown mimetype'
#         jp2_report['msg'] = msg
#         return jp2_report

#     # create jp2
#     jp2_filepath = os.path.join(tmp_dir, "sourcefile.jp2")
#     converted, jp2_msg = convert._tiff_to_jp2(prepped_filepath, jp2_filepath)
#     jp2_report['convert_tiff_to_jp2'] = {
#         'converted': converted,
#         'msg': jp2_msg
#     }

#     if not converted:
#         shutil.rmtree(tmp_dir)
#         return 
    
#     final_filepath = jp2_filepath
#     final_mimetype = 'image/jp2'