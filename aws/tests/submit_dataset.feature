Feature: Submit Dataset
    Repository for Materials Science Datasets.

    Scenario: Submit Dataset
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        When I submit the dataset

        Then a dynamo record should be created with the generated uuid
        And the dynamo record should be version 1.0
        And the previous_versions field should be empty
        And an automate flow started
        And the data destination should be the Petrel MDF directory
        And the search subject should be the uuid with the version
        And I should receive a success result with the generated uuid and version 1.0

    Scenario: Submit Dataset With Provided source_id
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        And I provide the source_id
        When I submit the dataset

        Then a dynamo record should be created with the provided source_id
        And the dynamo record should be version 1.0
        And an automate flow started
        And I should receive a success result with the generated uuid and version 1.0

    Scenario: Submit Test Dataset With Provided source_id
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        And I provide the source_id
        And I set the test flag to true
        When I submit the dataset

        Then a dynamo record should be created with the provided source_id modified to indicate test
        And the dynamo record should be version 1.0
        And an automate flow started
        And I should receive a success result with test source-id, the generated uuid and version 1.0


    Scenario: Attempt to submit when not member of globus group
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit
        And I'm not a member of the MDF globus group
        When I submit the dataset
        Then I should receive a failure result

    Scenario: Attempt to update another users record
        Given I'm authenticated with MDF
        And I have an update to another users record
        When I submit the dataset
        Then I should receive a failure result

    Scenario: Attempt to add a record with an existing source_id
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit with a source_id that already exists
        When I submit the dataset
        Then I should receive a failure result

    Scenario: Update a submitted dataset
        Given I'm authenticated with MDF
        And I have an update for an existing dataset
        When I submit the dataset

        Then a dynamo record should be created with the original source_id
        And the dynamo record should be version 1.1
        And the previous_versions field should be ['my dataset-1.0']
        And an automate flow started
        And I should receive a success result with the generated uuid and version 1.1

    Scenario: Update metadata only for a submitted dataset
        Given I'm authenticated with MDF
        And I have a metadata only update for an existing dataset
        When I submit the dataset
        Then a dynamo record should be created with the original source_id
        And the dynamo record should be version 1.1
        And an automate flow started that skips the file transfer
        And I should receive a success result

    Scenario: Update metadata only for a submitted dataset
        Given I'm authenticated with MDF
        And I have a metadata only update for an existing dataset
        When I submit the dataset
        Then a dynamo record should be created with the original source_id
        And the dynamo record should be version 1.1
        And an automate flow started that skips the file transfer
        And I should receive a success result

    Scenario: Submit Dataset and mint DOI
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit for an organization that mints DOIs
        When I submit the dataset

        Then a dynamo record should be created with the generated uuid
        And the dynamo record should be version 1.0
        And an automate flow started with a true mint DOI flag
        And the data destination should be the Petrel MDF directory
        And I should receive a success result with the generated uuid and version 1.0


    Scenario: Submit Dataset with invalid organization
        Given I'm authenticated with MDF
        And I have a new MDF dataset to submit for an organization that does not exist
        When I submit the dataset
        Then I should receive a failure result