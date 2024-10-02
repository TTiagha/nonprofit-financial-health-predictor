import os
from mistralai import Mistral

# Hardcoded API key (replace 'YOUR_API_KEY' with your actual API key)
api_key = 'RPsDrHBSNjhmPLUHmbKaZfySYEapVQEm'

model = 'open-mixtral-8x22b'  # Assuming the model name is 'mistral-8x-22B'

client = Mistral(api_key=api_key)

# Provide three examples to the model
examples = [
    {
        "role": "user",
        "content": "Nonprofit Name: Habitat for Humanity\nMission: Seeking to put God's love into action, Habitat for Humanity brings people together to build homes, communities and hope."
    },
    {
        "role": "assistant",
        "content": "Housing Development, Construction & Management"
    },
    {
        "role": "user",
        "content": "Nonprofit Name: American Red Cross\nMission: Prevents and alleviates human suffering in the face of emergencies by mobilizing the power of volunteers and the generosity of donors."
    },
    {
        "role": "assistant",
        "content": "Emergency Assistance"
    },
    {
        "role": "user",
        "content": "Nonprofit Name: Feeding America\nMission: To feed America's hungry through a nationwide network of member food banks and engage our country in the fight to end hunger."
    },
    {
        "role": "assistant",
        "content": "Food Banks & Pantries"
    }
]

def get_ntee_code_description(mission_description):
    # Create the conversation with examples
    messages = examples.copy()
    messages.append({
        "role": "user",
        "content": mission_description
    })
    
    response = client.chat.complete(
        model=model,
        messages=messages,
        max_tokens=50,
        temperature=0.0
    )
    
    description = response.choices[0].message.content.strip()
    return description

# Example usage:
mission_description = "Nonprofit Name: KANAWHA-CHARLESTON HUMANE\nMission: KCHA IS COMMITTED TO SAVING THE LIVES OF ANIMALS THROUGH SHELTERING, ADOPTION, FOSTER CARE, SPAY AND NEUTER PROGRAMS, CRUELTY PREVENTION, AND COMMUNITY OUTREACH."
description = get_ntee_code_description(mission_description)
print(description)
