
#### Enable ntpd on clients and servers.

Do this first so it can settle while you do the rest ...


#### Dependencies

Install systemtap software:

    # On RedHat/CentOS/Fedora:
    sudo yum install systemtap systemtap-runtime systemtap-sdt-devel

    # On Debian/Ubuntu:
    sudo apt-get install -y systemtap systemtap-runtime systemtap-sdt-dev


Add user to systemtap groups:

    sudo usermod -a -G stapusr,stapsys,stapdev <username>
    # relogin after this to obtain group privileges


#### Building server

    cd aerospike-server
    make USE_SYSTEMTAP=1 clean all


#### Collecting server events

    cd aerospike-server
    stap tools/systemtap/queries.stp -o /tmp/asd-`hostname`-stap.log


#### Annotate multiple concurrent trace files

    cd aerospike-server
    sort -n /tmp/*-stap.log | tools/systemtap/query_annotate 

