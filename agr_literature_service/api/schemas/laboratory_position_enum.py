from enum import Enum


class LabPosition(str, Enum):
    other = "other"
    co_pi = "co_pi"
    research_professor = "research_professor"
    md_vet = "md_vet"
    administrator = "administrator"
    animal_facility_staff = "animal_facility_staff"
    research_staff = "research_staff"
    technical_staff = "technical_staff"
    postdoc = "postdoc"
    graduate_student = "graduate_student"
    undergrad = "undergrad"
    masters_student = "masters_student"
    phd_student = "phd_student"
    high_school = "high_school"
