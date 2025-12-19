from typing import Optional
from langchain_mistralai import ChatMistralAI
from pydantic import BaseModel, Field

class Expense(BaseModel):
    amount: Optional[str] = Field(title="expense", description="The expense made on the transaction")
    merchant: Optional[str] = Field(title="merchant", description="The merchant name where the expense was made")
    currency: Optional[str] = Field(title="currency", description="The currency of the expense")