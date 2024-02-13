```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: focus-converter
spec:
  replicas: 1
  selector:
    matchLabels:
      app: focus-converter
  template:
    metadata:
      labels:
        app: focus-converter
    spec:
      containers:
        - name: focus-converter
          image: finopsfoundation/focus-converter:latest
          volumeMounts:
            - name: input-data
              mountPath: /data/input
            - name: output-data
              mountPath: /data/output
          env:
            - name: INPUT_DIR
              value: "/data/input"
            - name: OUTPUT_DIR
              value: "/data/output"
            - name: DATA_FORMAT
              value: "parquet"
            - name: PROVIDER
              value: "aws"
          command:
            - convert
            - --provider # auto detect
            - ${PROVIDER}
            - --data-path
            - ${INPUT_DIR}
            - --export-path
            - ${OUTPUT_DIR}
            - --data-format # use magic to read the headers, stdlib magic
            - ${DATA_FORMAT}
      volumes:
        # This is the volume that will be used to share the input data with the container
        # For different remote storage providers, the volume type and the volume name will change
        - name: input-data
          persistentVolumeClaim:
            claimName: input-data-pvc
        - name: output-data
          persistentVolumeClaim:
            claimName: output-data-pvc
```