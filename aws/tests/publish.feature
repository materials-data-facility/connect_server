# Created by bengal1 at 8/19/21
Feature: MDF Publish

  Scenario: Submit Test Dataset
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        And I mark the dataset as test
        When I submit the dataset
        Then a dynamo record should be created
        And an automate flow started
        And the only data destinations should be mdf_connect/test_files
        And I should receive a success result
