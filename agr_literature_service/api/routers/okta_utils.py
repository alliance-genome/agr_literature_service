from enum import Enum

from fastapi_okta import OktaUser


class OktaAccess(Enum):
    NO_ACCESS = 0
    ALL_ACCESS = 1
    SGD_ACCESS = 2
    RGD_ACCESS = 3
    MGI_ACCESS = 4
    ZFIN_ACCESS = 5
    XB_ACCESS = 6
    FB_ACCESS = 7
    WB_ACCESS = 8


OKTA_ACCESS_MOD_ABBR = {
    OktaAccess.NO_ACCESS: "no_access",
    OktaAccess.ALL_ACCESS: "all_access",
    OktaAccess.SGD_ACCESS: "SGD",
    OktaAccess.RGD_ACCESS: "RGD",
    OktaAccess.MGI_ACCESS: "MGI",
    OktaAccess.ZFIN_ACCESS: "ZFIN",
    OktaAccess.XB_ACCESS: "XB",
    OktaAccess.FB_ACCESS: "FB",
    OktaAccess.WB_ACCESS: "WB"
}


def get_okta_mod_access(user: OktaUser):
    if user.groups:
        for oktaGroup in user.groups:
            if oktaGroup.endswith('Developer'):
                return OktaAccess.ALL_ACCESS
            if oktaGroup == "SGDCurator":
                return OktaAccess.SGD_ACCESS
            if oktaGroup == "RGDCurator":
                return OktaAccess.RGD_ACCESS
            if oktaGroup == "MGICurator":
                return OktaAccess.MGI_ACCESS
            if oktaGroup == "ZFINCurator":
                return OktaAccess.ZFIN_ACCESS
            if oktaGroup == "XenbaseCurator":
                return OktaAccess.XB_ACCESS
            if oktaGroup == "FlyBaseCurator":
                return OktaAccess.FB_ACCESS
            if oktaGroup == "WormBaseCurator":
                return OktaAccess.WB_ACCESS
    elif user.cid == '0oa1cs2ineBqEFiD85d7':
        return OktaAccess.ALL_ACCESS
    return OktaAccess.NO_ACCESS
