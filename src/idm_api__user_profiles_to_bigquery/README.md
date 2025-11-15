
# check version required for pyodbc (macos)

odbcinst -j

## set flags
export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/[your version]/lib"
export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/[your version]/include"

i.e.
export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/2.3.12/lib"
export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/2.3.12/include"

## install pyodbc from source
pip install --no-binary :all: pyodbc

brew reinstall openssl@3
