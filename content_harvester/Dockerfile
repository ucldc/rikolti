FROM python:3

# Install dependencies
RUN apt-get update -qq && apt-get install -y libtiff-tools ffmpeg

# By default don't have permissions to modify PDFs with ImageMagick,
# so we need to comment out this line in the the policy
RUN sed -i 's/<policy domain="coder" rights="none" pattern="PDF" \/>/<!--<policy domain="coder" rights="none" pattern="PDF" \/>-->/' /etc/ImageMagick-6/policy.xml

WORKDIR /content_harvester

COPY requirements.txt ./

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./ /content_harvester

ENTRYPOINT [ "python", "by_registry_endpoint.py" ]
