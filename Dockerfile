FROM python:3.10

COPY focus_converter_base /focus_converter_base
WORKDIR /focus_converter_base

RUN pip install .

ENTRYPOINT ["focus-converter"]
