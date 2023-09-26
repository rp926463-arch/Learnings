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