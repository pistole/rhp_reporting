# RHP Reporting
This is a super-WIP proof of concept on a configuration-driven reporting framework

Current features include:
    * Automatic determination of facts, dimensions and rollups based on report columns and filters
    * Between/Equal/GE/GT/LT/LE/Substring filters
    * Join multiple facts together in CTEs and then re-join for final query

Excuse the mess and the non pythonic-python, it's been a while.

The testdata script will initialize and populate a testdb, and the test.py test will run every report specified in testconfig.yaml to verify they throw no exceptions.

