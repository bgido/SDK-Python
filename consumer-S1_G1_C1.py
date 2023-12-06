from __future__ import annotations
import asyncio
import time
import itertools
import cts

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
    async def msg_handler(msgs, error, context):
        try:
            for msg in msgs:
                print("message: ", msg.get_data_deserialized())
                await msg.ack()
                if error:
                    print(error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return
        
    try:
        memphis = Memphis()
        await memphis.connect(host=cts.Host, username=cts.User, password=cts.Pass, account_id=cts.AccountID, reconnect=True,max_reconnect=3000000, reconnect_interval_ms=1500, timeout_ms=5000)
      
        consumer = await memphis.consumer(
            station_name="events-1", 
            consumer_name="consumer-Barak-1", 
            consumer_group="Gido-1",
            pull_interval_ms=100, 
            batch_size=1000,
            batch_max_time_to_wait_ms=5000)
        consumer.set_context({"key": "value"})
        consumer.consume(msg_handler, consumer_partition_key="key-1")
        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.Event().wait()
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()
        
if __name__ == "__main__":
    asyncio.run(main())