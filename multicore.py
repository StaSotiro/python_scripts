# Example of how spliting a large CSV file to as many available CPUs as possible, making calculations
# and ultimately combining all sub process results in the main.
# You can switch and demonstrate time difference between running in one or multiple cores 
# by using the single_cpu variable.
# The file is 1000 rows, and appended on itself multiple times to demonstrate time difference.
# You can use times_to_mult variable to set the number of multiplications
# Used Mockaroo (https://www.mockaroo.com/) to generate dummy data in MOCK_DATA_orders.csv

import pandas as pd
import numpy as np
import multiprocessing

filePath = './samples/MOCK_DATA_orders.csv'
resultsFile = './results/area_totals.csv'
single_cpu = False
times_to_mult = 2000

def groupOrders(orders):
   area_totals = orders.groupby('area_id').agg(
       total_amount=('price', lambda x:  (x * orders.loc[x.index, 'quantity']).sum())
       ).reset_index()
   return area_totals

def groupFinalResults(area_totals_split):

    return area_totals_split.groupby(['area_id'])['total_amount'].sum().reset_index()


if __name__ == "__main__":

    # Read csv file
    orders = pd.read_csv(filePath)

    orders_repeated = orders
    # Make it larger
    if times_to_mult > 1:
        orders_repeated = pd.concat([orders] * times_to_mult, ignore_index=True)
    print('Total rows: ', len(orders_repeated))

    area_totals = None

    # Try it in a single thread or multiple threads
    if single_cpu:
        print('\n\nRunning single core!\n\n')
        area_totals = groupOrders(orders_repeated)
    else:
        cpus = multiprocessing.cpu_count()
        print('\n\nRunning multiple cores \nNumber of available CPUs: ', cpus , '\n\n')

        orders_split = np.array_split(orders_repeated, cpus)
        with multiprocessing.Pool() as pool:
          result = pool.map(groupOrders, orders_split)
        
        # Combine results
        area_totals_split = pd.concat(result, ignore_index=True)
        area_totals = groupFinalResults(area_totals_split)

    area_totals['total_amount'] = area_totals['total_amount'].apply(lambda x: round(x, 2))
    area_totals.to_csv(resultsFile, sep=',', header=True, index=False)

    print('Program finished!')
