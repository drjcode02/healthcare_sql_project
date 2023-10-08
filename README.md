# Healthcare sql project in Snowflake

Source of data set in Kaggle - https://www.kaggle.com/datasets/thedevastator/optimizing-operating-room-utilization

Data Summary : Hospital operations rooms utilization data for the booked procedures.

Analysis objective: To do exploratory analysis and providing insights on usage of operation rooms to encounter the procedures.

Data definition :

Encounter ID - Unique ID for the service rendered.
Date - date when the service rendered.
OR Suite - Operation room number.
CPT Code - Unique number for the procedures to treat in the OR.
CPT description - Description of the CPT Code.
Booked Time in minutes - Its pre booked estimated time to operate the procedure.
OR schedule - Operating room scheduled time.
Wheels in - Time to take the patient for the procedures to Operation room.
Start time - Procedure start time.
End time - Procedure end time.
Wheels out - Time to take out the patient from operation room.

Questions :

How many number of operations serviced by day wise?

How many unique operations serviced ?

What's the common procedures serviced?

How many number of procedures delayed from booked time ?

What procedure most delayed from booked time ?

What is the average time to take the patient from scheduled time to wheels in time ?

Which procedure taken longest and short time  from start and end time ?

Which type of service takes long time to complete the operation?

Compare the total time taken with the previous years for the procedure 'Arthroplasty, knee, hinge prothesis' ?

