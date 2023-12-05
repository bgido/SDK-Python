from __future__ import annotations
import asyncio
import time
import cts
import itertools

import subprocess
package_name = 'memphis-py-beta'
try:
    subprocess.check_call(['pip', 'uninstall', '-y', package_name])
    print(f'Successfully uninstalled {package_name}')
    subprocess.check_call(['pip', 'install', package_name])
    print(f'Successfully installed {package_name}')
except subprocess.CalledProcessError as e:
    print(f'Error installing {package_name}: {e}')

from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError

async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host=cts.Host, username=cts.User, password=cts.Pass, account_id=cts.AccountID, reconnect=True, max_reconnect=3000000, reconnect_interval_ms=3000, timeout_ms=9000)
        await memphis.station(name="events-1", partitions_number=30, dls_station = "DLS")
        producer = await memphis.producer(station_name="events-1", producer_name="producer-Barak-1") # you can send the message parameter as dict as well
        headers = Headers()
        headers.add("key", "value") 
        for i in itertools.count():
            try:
                await producer.produce(bytearray("Message #" + str(i) + ": Key-1 partition key-1", "utf-8"), headers=headers, producer_partition_key="key-1", nonblocking=True)
                if i == 100:
                    time.sleep(5)
                
            except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
                print(e)
        
    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)
        
    finally:
        await memphis.close()
        
if __name__ == "__main__":
    asyncio.run(main())


