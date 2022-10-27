from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_database_md5data


class TestMd5sumUtils:

    def test_load_database_md5data(mods):
        md5dict = load_database_md5data(["FB", "WB", "ZFIN", "SGD", "MGI", "RGD", "XB", "PMID"])
        assert(md5dict["FB"]["FB:FBrf0000001"] == "2b3eff3f69156bea6a51d8974ada32fc")
        assert(md5dict["WB"]["WB:WBPaper00014325"] == "d2cf7adf3fb1820d69ac3114de597358")
        assert(md5dict["ZFIN"]["ZFIN:ZDB-PUB-130524-1"] == "a4e2fd376f28c361b8f9e06602b5e440")
        assert(md5dict["SGD"]["SGD:S000156742"] == "37f46a22b062d87509289da618864464")
        assert(md5dict["MGI"]["MGI:5883352"] == "6926940c28bcf91bdbdd6b43484a3e5b")
        assert(md5dict["RGD"]["RGD:1601459"] == "387b1acee3ba2fd0a79acba6d92e8bf6")
        assert(md5dict["XB"]["Xenbase:XB-ART-32622"] == "9177c6f32fb8a80ef5955543b9dafde6")
        assert(md5dict["PMID"]["PMID:21423809"] == "8c8207da09fce9efa4cbf1b359e2eefd")
