from agr_literature_service.api.models import ReferenceModel, ModModel


def referencefiles_present(reference_obj: ReferenceModel, mod_obj: ModModel):
    return len([reffile for reffile in reference_obj.referencefiles for referencefile_mod in reffile.referencefile_mods
                if referencefile_mod.mod.abbreviation == mod_obj.abbreviation]) > 0
