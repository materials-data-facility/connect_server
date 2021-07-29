Feature: Submit Dataset
    Repository for Materials Science Datasets.

    Scenario: Submit Dataset
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        When I submit the dataset

        Then a dynamo record should be created
        And an automate flow started
        And I should receive a success result

    Scenario: Attempt to update another users record
        Given I'm authenticated with MDF
        And I have an update to another users record

        When I submit the dataset

        Then I should receive a failure result
