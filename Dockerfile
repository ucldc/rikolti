FROM python:3.9

RUN /usr/local/bin/python -m pip install --upgrade pip

RUN adduser rikolti
USER rikolti
WORKDIR /home/rikolti

ENV PATH="$PATH:/home/rikolti/.local/bin"

ENTRYPOINT ["tail", "-f", "/dev/null"]