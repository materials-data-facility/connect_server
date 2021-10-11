from organization import Organization


class TestOrganization:
    def test_from_schema_repo(self):
        # Find by canonical name
        nrel = Organization.from_schema_repo("National Renewable Energy Laboratory")
        assert nrel
        assert nrel.canonical_name == "National Renewable Energy Laboratory"

        # Find by Alias
        nrel2 = Organization.from_schema_repo("NREL")
        assert nrel2
        assert nrel2.canonical_name == "National Renewable Energy Laboratory"
