from typing import Dict, List
from portkey_ai import Portkey
from common.models.base.base_model import BaseModel

class PortkeyModel(BaseModel):
    def __init__(self, api_key, config):
        self.client = Portkey(
            api_key=api_key,
            config=config
        )
        
    def do_completion(self, data: Dict):
        completion = self.client.prompts.completions.create(
            prompt_id=data["prompt_id"],
            variables=data["variables"]
        )
        return completion["choices"][0].message.content

    def get_prompt_messages(self, prompt_id: str, variables: Dict):
        render = self.client.prompts.render(
            prompt_id=prompt_id,
            variables=variables
        )
        return render.data.messages

    def do_chat_completion(self, messages: List[Dict], model, temperature):
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        return response.choices[0].message.content
