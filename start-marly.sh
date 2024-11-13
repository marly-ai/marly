#!/bin/bash

if [ ! -x "$0" ]; then
    echo -e "${RED}Error: Script doesn't have execute permissions${NC}"
    echo "Please run: chmod +x $0"
    exit 1
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}=== Checking System Requirements ===${NC}"

if [ -f "examples/scripts/requirements.txt" ]; then
    
    if command -v pip3 &> /dev/null; then
        PIP_CMD="pip3"
    elif command -v pip &> /dev/null; then
        PIP_CMD="pip"
    else
        echo -e "${RED}Error: Neither pip nor pip3 is installed${NC}"
        exit 1
    fi
    
    missing_packages=0
    while IFS= read -r package || [ -n "$package" ]; do
        [[ $package =~ ^[[:space:]]*$ || $package =~ ^#.*$ ]] && continue
        
        package_name=$(echo "$package" | cut -d'=' -f1 | cut -d'>' -f1 | cut -d'<' -f1 | tr -d ' ')
        
        if ! $PIP_CMD show "$package_name" >/dev/null 2>&1; then
            echo -e "${RED}❌ Missing package: ${package_name}${NC}"
            missing_packages=1
        else
            echo -e "${GREEN}✓ ${package_name} is installed${NC}"
        fi
    done < "examples/scripts/requirements.txt"

    if [ $missing_packages -eq 1 ]; then
        echo -e "\n${RED}Error: Missing Python requirements${NC}"
        echo -e "Please install required packages with:"
        echo -e "$PIP_CMD install -r examples/scripts/requirements.txt"
        exit 1
    fi
else
    echo -e "${RED}Error: examples/scripts/requirements.txt not found${NC}"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
else
    echo -e "${GREEN}✓ Docker is installed${NC}"
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
else
    echo -e "${GREEN}✓ Docker Compose is installed${NC}"
fi

if ! docker info &> /dev/null; then
    echo -e "\n${RED}Error: Docker daemon is not running${NC}"
    echo "Please start Docker daemon first"
    exit 1
else
    echo -e "${GREEN}✓ Docker daemon is running${NC}"
fi

echo -e "\n${BLUE}=== Attempting to load .env file... ===${NC}"

if [ ! -f .env ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo -e "Please create a .env file with your provider credentials"
    exit 1
fi

set -a
source .env >/dev/null 2>&1
set +a

provider_configured=false

if [ ! -z "${AZURE_OPENAI_API_KEY}" ]; then
    provider_configured=true
    missing_vars=0
    
    azure_vars=(
        "AZURE_RESOURCE_NAME"
        "AZURE_DEPLOYMENT_ID"
        "AZURE_MODEL_NAME"
        "AZURE_API_VERSION"
        "AZURE_OPENAI_API_KEY"
        "AZURE_ENDPOINT"
    )
    
    for var in "${azure_vars[@]}"; do
        if [ -z "${!var}" ]; then
            echo -e "${RED}❌ Missing ${var}${NC}"
            missing_vars=1
        else
            echo -e "${GREEN}✓ ${var} is set${NC}"
        fi
    done
    
    if [ $missing_vars -eq 1 ]; then
        echo -e "${RED}Error: Azure OpenAI requires all related environment variables${NC}"
        exit 1
    fi
fi

if [ ! -z "${OPENAI_API_KEY}" ]; then
    provider_configured=true
    echo -e "${GREEN}✓ OpenAI configured${NC}"
fi

if [ ! -z "${CEREBRAS_API_KEY}" ]; then
    provider_configured=true
    echo -e "${GREEN}✓ Cerebras configured${NC}"
fi

if [ ! -z "${GROQ_API_KEY}" ]; then
    provider_configured=true
    echo -e "${GREEN}✓ Groq configured${NC}"
fi

if [ ! -z "${MISTRAL_API_KEY}" ]; then
    provider_configured=true
    echo -e "${GREEN}✓ Mistral configured${NC}"
fi

if [ "$provider_configured" = false ]; then
    echo -e "${YELLOW}Warning: No AI Model provider credentials found in .env file${NC}"
    echo -e "These are some of the model providers that are supported:"
    echo -e "- Azure OpenAI (AZURE_OPENAI_API_KEY + related vars)"
    echo -e "- OpenAI (OPENAI_API_KEY)"
    echo -e "- Cerebras (CEREBRAS_API_KEY)"
    echo -e "- Groq (GROQ_API_KEY)"
    echo -e "- Mistral (MISTRAL_API_KEY)"
    echo -e "\n${YELLOW}Are you sure you want to continue without any of these providers configured? (yes/no)${NC}"
    read -r answer
    
    if [ "$(echo $answer | tr '[:upper:]' '[:lower:]')" = "yes" ]; then
        echo -e "${YELLOW}Continuing without provider configuration. Please ensure your desired AI model provider is supported by Marly.${NC}"
    else
        echo -e "${RED}Please configure at least one provider in your .env file${NC}"
        exit 1
    fi
fi


if ! grep -q "^LANGCHAIN_TRACING_V2=true" .env; then
    echo -e "${YELLOW}Warning: LANGCHAIN_TRACING_V2 is not enabled${NC}"
    echo -e "\n${YELLOW}Are you sure you want to continue without LangSmith tracing? (yes/no)${NC}"
    read -r answer
    
    if [ "$(echo $answer | tr '[:upper:]' '[:lower:]')" = "yes" ]; then
        echo -e "${YELLOW}Continuing without LangSmith tracing...${NC}"
    else
        echo -e "${RED}Please configure LangSmith tracing in your .env file${NC}"
        echo -e "\nRequired variables:"
        echo -e "LANGCHAIN_TRACING_V2=true"
        echo -e "LANGCHAIN_ENDPOINT=https://api.smith.langchain.com"
        echo -e "LANGCHAIN_API_KEY=<your-api-key>"
        echo -e "LANGCHAIN_PROJECT=<your-project-name>"
        exit 1
    fi
else
    echo -e "\n${BLUE}Checking LangSmith configuration:${NC}"
    missing_vars=0
    
    langsmith_vars=(
        "LANGCHAIN_ENDPOINT"
        "LANGCHAIN_API_KEY"
        "LANGCHAIN_PROJECT"
    )
    
    for var in "${langsmith_vars[@]}"; do
        if ! grep -q "^${var}=" .env; then
            echo -e "${RED}❌ ${var} must be explicitly set in .env file${NC}"
            missing_vars=1
        else
            echo -e "${GREEN}✓ ${var} is set${NC}"
        fi
    done
    
    if [ $missing_vars -eq 1 ]; then
        echo -e "${RED}Error: LangSmith tracing requires all related environment variables${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ LangSmith tracing has been correctly configured${NC}"
    fi
fi

echo -e "\n${BLUE}=== Checking for Running Containers ===${NC}"

if docker ps --format '{{.Names}}' | grep -qE '(pipeline-1|extraction-1|transformation-1)'; then
    echo -e "${RED}Error: Marly containers are already running${NC}"
    echo -e "\nRunning containers:"
    docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E '(pipeline-1|extraction-1|transformation-1)'
    exit 1
else
    echo -e "\n${GREEN}✓ All checks passed! Starting services...${NC}"
    docker-compose up --build
fi