from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from agr_literature_service.api.crud import dataset_crud
from agr_literature_service.api.schemas.dataset_schema import Dataset, DatasetCreate, DatasetUpdate
from agr_literature_service.api.database.base import get_db

router = APIRouter()

@router.post("/datasets/", response_model=Dataset)
def create_dataset(dataset: DatasetCreate, db: Session = Depends(get_db)):
    return dataset_crud.create_dataset(db, dataset)


@router.get("/datasets/{dataset_id}", response_model=Dataset)
def read_dataset(dataset_id: int, db: Session = Depends(get_db)):
    db_dataset = dataset_crud.get_dataset(db, dataset_id)
    if db_dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return db_dataset


@router.get("/datasets/", response_model=List[Dataset])
def read_datasets(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    datasets = dataset_crud.get_datasets(db, skip=skip, limit=limit)
    return datasets


@router.put("/datasets/{dataset_id}", response_model=Dataset)
def update_dataset(dataset_id: int, dataset: DatasetUpdate, db: Session = Depends(get_db)):
    db_dataset = dataset_crud.update_dataset(db, dataset_id, dataset)
    if db_dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return db_dataset


@router.delete("/datasets/{dataset_id}", response_model=bool)
def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    success = dataset_crud.delete_dataset(db, dataset_id)
    if not success:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return success
