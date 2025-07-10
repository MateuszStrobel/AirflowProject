'from airflow.decorators import dag, task

import pendulum


@dag(

   'scheduling_demo_1',

   schedule = '@hourly',

   start_date = pendulum.datetime(2023,3,10),

   catchup = True

)

def scheduling_demo_1():


   @task

   def _my_task():

       print('Hello World')


   _my_task()


scheduling_demo_1()'