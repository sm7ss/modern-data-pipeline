from pydantic import BaseModel, Field

class read_n_rows_sample_validation(BaseModel): 
    n_rows_sample: int = Field(ge=100)

class general_margin_validation(BaseModel):
    os_margin: float= Field(ge=0.2, le=0.4)

class default_parquet_validation(BaseModel): 
    parquet_overhead_factor: float= Field(ge=1.3, le=2.5)

class default_csv_validation(BaseModel): 
    csv_bytes_factor: int= Field(ge=8, le=256)
    csv_overhead: float= Field(ge=1.6, le=3.0)

class input_path_config_validation(BaseModel): 
    read_n_rows_sample: read_n_rows_sample_validation
    general_margin: general_margin_validation
    default_parquet_factors: default_parquet_validation
    default_csv_factors: default_csv_validation

