from agr_literature_service.api.models import ReferenceModel, ModModel


def referencefiles_present(reference_obj: ReferenceModel, mod_obj: ModModel):
    """Whether the reference has a file for the MOD. A file with no MOD (PMC) belongs to every
    MOD, as in the file upload SQL elsewhere; it used to raise AttributeError here."""
    return any(referencefile_mod.mod is None or referencefile_mod.mod.abbreviation == mod_obj.abbreviation
               for reffile in reference_obj.referencefiles for referencefile_mod in reffile.referencefile_mods)
