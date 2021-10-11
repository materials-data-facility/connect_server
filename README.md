# MDF Connect
The Materials Data Facility Connect service is the ETL flow to deeply index datasets into MDF Search. It is not intended to be run by end-users. To submit data to the MDF, visit the [Materials Data Facility](https://materialsdatafacility.org).


# Running Tests
To run the tests first make sure that you are running python 3.7.10. Then install the dependencies:

    $ cd aws/tests
    $ pip3 install -r requirements-test.txt

Now you can run the tests using the command:

    $ PYTHONPATH=.. python -m pytest --ignore schemas

# Support
This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was performed under the following financial assistance award 70NANB19H005 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the Center for Hierarchical Materials Design (CHiMaD). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".
