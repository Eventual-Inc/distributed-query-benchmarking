kubectl exec $POD_NAME -- bash -c 'cd /tmp && \
    git clone https://github.com/Eventual-Inc/distributed-query-benchmarking.git && \
    cd distributed-query-benchmarking && \
    pip install -r requirements.txt \
    pip install -e .
'
