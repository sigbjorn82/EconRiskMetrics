"""
    EnvLoader.jl

Simple utility to load environment variables from .env files.
Works around issues with spaces in directory paths.
"""

module EnvLoader

export load_env

"""
    load_env(env_file::String=".env")

Load environment variables from a .env file.

# Arguments
- `env_file::String`: Path to .env file (default: ".env" in current directory)

# Example
```julia
load_env()  # Loads from .env in current directory
load_env("/path/to/.env")  # Load from specific file
```
"""
function load_env(env_file::String=".env")
    if !isfile(env_file)
        @warn "Environment file not found: $env_file"
        return false
    end

    try
        for line in readlines(env_file)
            # Skip empty lines and comments
            line = strip(line)
            if isempty(line) || startswith(line, '#')
                continue
            end

            # Parse KEY=VALUE
            if contains(line, '=')
                key, value = split(line, '=', limit=2)
                key = strip(key)
                value = strip(value)

                # Set environment variable
                ENV[key] = value
            end
        end
        return true
    catch e
        @warn "Error loading environment file: $e"
        return false
    end
end

end # module EnvLoader
