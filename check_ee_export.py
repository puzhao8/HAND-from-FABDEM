import ee

# Authenticate and initialize the Earth Engine API
# ee.Authenticate()
ee.Initialize()

def print_task_statuses(keyWords='SWE'):
    # Retrieve the list of tasks
    tasks = ee.batch.Task.list()
    tasks_flt = [task for task in tasks if keyWords in task.config['description'] ]
    # Iterate through the tasks and print their statuses
    for task in tasks:
        task_id = task.id
        task_type = task.config['type']
        task_state = task.state
        print(f"Task ID: {task_id}")
        print(f"Task Type: {task_type}")
        print(f"Task State: {task_state}")
        print("-" * 40)

# Call the function to print the task statuses
print_task_statuses()
