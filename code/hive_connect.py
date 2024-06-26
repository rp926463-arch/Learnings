from pyhive import hive
import sasl
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift_sasl import TSaslClientTransport

# Set the connection parameters
hive_host = "your_hive_host"
hive_port = your_hive_port
hive_principal = "your_hive_principal"  # Example: "hive/_HOST@REALM"

# Create a connection to Hive
transport = TSocket.TSocket(hive_host, hive_port)
sasl_client = sasl.Client()
sasl_client.setAttr("host", hive_host)
sasl_client.init()

transport = TSaslClientTransport(transport, mechanism="GSSAPI", sasl=sasl_client)
transport.open()

# Establish the Hive connection
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    username=hive_principal,
    auth=transport,
    database="your_hive_database"  # Replace with your database name
)

# Execute Hive queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_hive_table")
results = cursor.fetchall()

# Close the connection when done
conn.close()



from pyhive import hive
from thrift.transport import TTransport
from thrift.transport.TSocket import TSocket
from thrift_sasl import TSaslClientTransport

# Set the connection parameters
hive_host = "your_hive_host"
hive_port = your_hive_port
hive_principal = "your_hive_principal"  # Example: "hive/_HOST@REALM"
hive_database = "your_hive_database"  # Replace with your database name

# Create a TTransport socket
transport = TSocket(hive_host, hive_port)
transport = TTransport.TBufferedTransport(transport)

# Create a SASL client
sasl_client = TSaslClientTransport(
    transport,
    mechanism="GSSAPI",
    service="hive",
    host=hive_host,
    username=hive_principal,
)

# Open the transport
sasl_client.open()

# Establish the Hive connection
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    auth=sasl_client,
    database=hive_database,
)

# Execute Hive queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_hive_table")
results = cursor.fetchall()

# Close the connection when done
conn.close()










from pyhive import hive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift_sasl import TSaslClientTransport

# Set the connection parameters
hive_host = "your_hive_host"
hive_port = your_hive_port
hive_principal = "your_hive_principal"  # Example: "hive/_HOST@REALM"
hive_database = "your_hive_database"  # Replace with your database name

# Create a connection to Hive
transport = TSocket.TSocket(hive_host, hive_port)
transport = TTransport.TBufferedTransport(transport)

# Set up the SASL client
sasl_client = TSaslClientTransport(
    transport,
    mechanism='GSSAPI',  # Kerberos
    username=hive_principal,
    service="hive",  # Hive service name
)

# Open the transport
sasl_client.open()

# Establish the Hive connection
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    auth=sasl_client,
    database=hive_database,
)

# Execute Hive queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_hive_table")
results = cursor.fetchall()

# Close the connection when done
conn.close()















import os
from pyhive import hive
from thrift.transport import TTransport
from thrift.transport.TSocket import TSocket
from thrift_sasl import TSaslClientTransport

# Set the connection parameters
hive_host = "your_hive_host"
hive_port = your_hive_port
hive_principal = "your_hive_principal"  # Example: "hive/_HOST@REALM"
hive_database = "your_hive_database"  # Replace with your database name

# Set the KERBEROS_TICKET_CACHE environment variable
os.environ['KERBEROS_TICKET_CACHE'] = '/tmp/krb5cc_1000'  # Replace with your ticket cache path

# Create a connection to Hive
transport = TSocket(hive_host, hive_port)
transport = TTransport.TBufferedTransport(transport)

# Open the transport
transport.open()

# Create a SASL client for Kerberos authentication
sasl_client = TSaslClientTransport(
    transport,
    mechanism='GSSAPI',
    sasl_protocol=None,
    service="hive",  # Hive service name
)

# Establish the Hive connection
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    auth=sasl_client,
    database=hive_database,
)

# Execute Hive queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM your_hive_table")
results = cursor.fetchall()

# Close the connection when done
conn.close()
