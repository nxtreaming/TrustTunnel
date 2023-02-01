# syntax=docker/dockerfile:1
FROM bench-ls

ARG CONAN_VER=1.53.0
ARG GOLANG_VER=1.18.3
ARG LLVM_MAJOR_VER=14

ARG VPN_LIBS_DIR=vpn-libs
ARG NLC_URL="https://github.com/AdguardTeam/NativeLibsCommon.git"
ARG DNS_LIBS_URL="https://github.com/AdguardTeam/DnsLibs.git"

RUN apt install -y iptables ninja-build && \
    pip install conan~=$CONAN_VER && \
    ## Install LLVM
    curl -O https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh $LLVM_MAJOR_VER && \
    rm ./llvm.sh && \
    apt install -y libc++-$LLVM_MAJOR_VER-dev && \
    ln -s libc++abi.so.1 /usr/lib/llvm-$LLVM_MAJOR_VER/lib/libc++abi.so && \
    ## Install Go
    curl https://dl.google.com/go/go${GOLANG_VER}.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
    rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tar.gz && rm /tmp/go.tar.gz
ENV PATH="/usr/lib/llvm-$LLVM_MAJOR_VER/bin:/usr/local/go/bin:$PATH"

COPY $VPN_LIBS_DIR /bench/$VPN_LIBS_DIR
COPY bootstrap_conan_deps.py /bench/

RUN pipreqs . && \
    pip install -r requirements.txt && \
    rm requirements.txt && \
    ./bootstrap_conan_deps.py $VPN_LIBS_DIR $NLC_URL $DNS_LIBS_URL && \
    mkdir -p /bench/$VPN_LIBS_DIR/build && \
    cd /bench/$VPN_LIBS_DIR/build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release -G "Ninja" \
        -DCMAKE_C_COMPILER="clang" \
        -DCMAKE_CXX_COMPILER="clang++" \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++" && \
    ninja standalone_client && \
    mv ./core/standalone_client /bench/

COPY entrypoint.sh /bench/

WORKDIR /bench/
ENTRYPOINT ["./entrypoint.sh"]
CMD ["endpoint_hostname", "endpoint_ip", "protocol", "mode", "socks_port_first", "socks_port_last"]
