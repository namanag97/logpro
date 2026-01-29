#!/bin/bash
# Pre-tool-use hook: before rm/rm -rf, commit any modified files being deleted.

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // empty')

# Check if the command contains rm
if ! echo "$command" | grep -qE '(^|\s|;|&&|\|\|)(sudo\s+)?rm\s'; then
  exit 0
fi

# Extract file paths from the rm command.
# Strip the rm command and its flags, leaving just the file arguments.
files=$(echo "$command" | grep -oE '(sudo\s+)?rm\s+[^;|&]*' | sed 's/^\(sudo\s\+\)\?rm\s\+//' | sed 's/-[a-zA-Z]*\s*//g' | tr ' ' '\n' | sed '/^$/d')

if [ -z "$files" ]; then
  exit 0
fi

committed=0
while IFS= read -r file; do
  # Skip empty lines and globs/wildcards (let those through)
  [ -z "$file" ] && continue
  echo "$file" | grep -q '[*?]' && continue

  # Check if file has uncommitted changes (modified, staged, or untracked)
  status=$(git status --porcelain -- "$file" 2>/dev/null)
  if [ -n "$status" ]; then
    git add -- "$file" 2>/dev/null
    committed=1
  fi
done <<< "$files"

if [ "$committed" -eq 1 ]; then
  git commit -m "Auto-commit before rm: save modified files" 2>/dev/null
  echo "Committed modified files before deletion." >&2
fi

exit 0
