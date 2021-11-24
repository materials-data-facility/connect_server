from organization import Organization


class TestOrganization:
    def test_from_schema_repo(self):
        # Find by canonical name
        midas = Organization.from_schema_repo("AFRL Additive Manufacturing Challenge")
        assert midas
        assert midas.canonical_name == "AFRL Additive Manufacturing Challenge"

        # Find by Alias
        midas2 = Organization.from_schema_repo("MIDAS")
        assert midas2
        assert midas2.canonical_name == "AFRL Additive Manufacturing Challenge"
