#! /bin/bash

usage()
{
    # Display Help
    echo
    echo "A helper script to build and publish cmdrvl-mcp-server-apache-airflow"
    echo
    echo "Syntax: python_build.sh [options]"
    echo
    echo "Options:"
    echo
    echo "h             Print this Help."
    echo "v <version>   Set the version (default uses the version specified in pyproject.toml)"
    echo "l             List the current version"
    echo "i             Build inline (default is to copy to a tmp directory)"
    echo "p             Publishes the built module to the private code artifact repository"
    echo "o             Overwrite existing version (if publishing)"
    echo "a             The name of the AWS Profile in ~/.aws/credentials that represents the account hosting the Python repo."
    echo "              If this is not passed, the default profile is used. This is used to retrieve the temporay auth token."
    echo "              Even if this fails, the token could still be valid as it has a 12hr expiration."
    echo
    echo "Dependencies:"
    echo "  The script will automatically install required Python packages (build, twine) if missing."
    echo "  You need AWS CLI installed and configured with appropriate credentials."
    echo
}

error_and_exit()
{
    exit_code=${1}
    error_msg=${2}

    echo && \
    echo "${error_msg}" && \
    echo && \
    exit ${exit_code}
}

if [ ${#} -gt 0 ]; then
    while getopts ":h" option; do
        case ${option} in
            h) # display Help
                usage
                exit 0
        esac
    done
fi

MODULE="cmdrvl-mcp-server-apache-airflow"

INLINE="FALSE"
VERSION=
LIST=
PUBLISH=
SOURCE_DIR="$(dirname "$(realpath "$0")")"
PUBLISH_DIR=
AWS_PROFILE_PYTHON_REPO=
OVERWRITE=

####
#   Check Dependencies
####
check_dependencies()
{
    echo "  Checking dependencies..."

    # Check for AWS CLI
    if ! command -v aws &> /dev/null; then
        error_and_exit 204 "ERROR: AWS CLI is not installed. Please install AWS CLI first."
    fi

    # Check for Python
    if ! command -v python &> /dev/null; then
        error_and_exit 205 "ERROR: Python is not installed or not in PATH."
    fi

    # Check for pip
    if ! command -v pip &> /dev/null; then
        error_and_exit 206 "ERROR: pip is not installed or not in PATH."
    fi

    echo "  ✓ Dependencies check passed"
}

####
#   Builds a Python Distribution
####
build()
{
    # Check dependencies first
    check_dependencies

    # Ensure we're logged into CodeArtifact for pip
    echo "  Ensuring CodeArtifact login for pip..."
    aws codeartifact login ${AWS_PROFILE_PYTHON_REPO_OPTION} --tool pip --repository foodtruck.python.artifact.repository --domain foodtruck-codeartifact-domain --domain-owner 812753953378 --region us-east-1
    if [ ${?} -ne 0 ]; then
        echo "  ❌ FAILED: CodeArtifact login for pip failed"
        error_and_exit 102 "ERROR: Failed to login to CodeArtifact for pip"
    fi
    if [ ! -d "${SOURCE_DIR}" ]; then
        error_and_exit 201 "ERROR: SOURCE_DIR is not a valid directory"
    fi

    if [ "${INLINE}" == "FALSE" ]; then
        TMP_DIR="/tmp/${MODULE}_build"

        # Cleanup
        echo "  Cleanup old build dir..."
        rm -rf ${TMP_DIR}

        # Also cleanup source dist folder to avoid stale wheels
        rm -rf ${SOURCE_DIR}/dist
        rm -rf ${SOURCE_DIR}/build
        rm -rf ${SOURCE_DIR}/*.egg-info
        find ${SOURCE_DIR} -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true

        # Create working dir
        echo "  Setup new build dir..."
        mkdir -p ${TMP_DIR}

        # Copy module into working dir
        echo "  Copying module source into build dir..."
        cp -LR ${SOURCE_DIR} ${TMP_DIR}

        BUILD_DIR="${TMP_DIR}/$(basename ${SOURCE_DIR})"
    else
        # Cleanup any prior builds
        rm -rf ${SOURCE_DIR}/dist
        rm -rf ${SOURCE_DIR}/build
        rm -rf ${SOURCE_DIR}/*.egg-info
        find ${SOURCE_DIR} -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true

        BUILD_DIR=${SOURCE_DIR}
    fi

    # Edit the version file to override the version
    cd ${BUILD_DIR}
    if [ "${VERSION}" != "" ]; then
        echo "  Overriding version..."
        if [ -f "pyproject.toml" ]; then
            # Use sed in a portable way (works on both Linux and macOS)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                sed -i "" "s/^version = \".*\"/version = \"${VERSION}\"/g" ${BUILD_DIR}/pyproject.toml
            else
                sed -i "s/^version = \".*\"/version = \"${VERSION}\"/g" ${BUILD_DIR}/pyproject.toml
            fi
        else
            echo "  ❌ FAILED: pyproject.toml not found"
            error_and_exit 207 "ERROR: pyproject.toml not found in build directory"
        fi
    fi

    # Check and install build dependencies
    echo "  Checking build dependencies..."
    if ! python -c "import build" 2>/dev/null; then
        echo "  Installing build dependency..."
        pip install build
        INSTALL_EXIT_CODE=${?}
        if [ ${INSTALL_EXIT_CODE} -ne 0 ]; then
            echo "  ❌ FAILED: Build dependency installation failed with exit code ${INSTALL_EXIT_CODE}"
            error_and_exit 203 "ERROR: Failed to install build dependency (exit code: ${INSTALL_EXIT_CODE}). Please run: pip install build"
        fi
        echo "  ✅ SUCCESS: Build dependency installed"
    fi

    # Check and install hatchling (required for this package's build backend)
    # Use older version to avoid Metadata-Version 2.4 issues with CodeArtifact
    # Hatchling 1.x generates Metadata-Version 2.2, which is compatible
    echo "  Checking hatchling version..."

    # Check if hatchling is installed and if it's version 2.x (which generates Metadata-Version 2.4)
    HATCHLING_VERSION=$(pip show hatchling 2>/dev/null | grep "^Version:" | awk '{print $2}' || echo "")

    if [ -z "${HATCHLING_VERSION}" ] || python -c "from packaging import version; exit(0 if version.parse('${HATCHLING_VERSION}') >= version.parse('2.0.0') else 1)" 2>/dev/null; then
        if [ -n "${HATCHLING_VERSION}" ]; then
            echo "  Current hatchling version: ${HATCHLING_VERSION} (too new, will downgrade)"
        else
            echo "  Hatchling not found, installing compatible version..."
        fi
        echo "  Installing compatible hatchling version (<2.0.0) to avoid Metadata-Version 2.4 issues..."
        pip install "hatchling<2.0.0" --upgrade
        INSTALL_EXIT_CODE=${?}
        if [ ${INSTALL_EXIT_CODE} -ne 0 ]; then
            echo "  ❌ FAILED: Hatchling installation failed with exit code ${INSTALL_EXIT_CODE}"
            error_and_exit 208 "ERROR: Failed to install hatchling (exit code: ${INSTALL_EXIT_CODE}). Please run: pip install \"hatchling<2.0.0\""
        fi
        INSTALLED_VERSION=$(pip show hatchling 2>/dev/null | grep "^Version:" | awk '{print $2}')
        echo "  ✅ SUCCESS: Compatible hatchling version ${INSTALLED_VERSION} installed"
    else
        echo "  ✅ SUCCESS: Compatible hatchling version ${HATCHLING_VERSION} already installed"
    fi

    # Ensure packaging is available for version comparison
    if ! python -c "import packaging" 2>/dev/null; then
        echo "  Installing packaging dependency..."
        pip install packaging
        INSTALL_EXIT_CODE=${?}
        if [ ${INSTALL_EXIT_CODE} -ne 0 ]; then
            echo "  ❌ FAILED: Packaging dependency installation failed with exit code ${INSTALL_EXIT_CODE}"
            error_and_exit 206 "ERROR: Failed to install packaging dependency (exit code: ${INSTALL_EXIT_CODE}). Please run: pip install packaging"
        fi
        echo "  ✅ SUCCESS: Packaging dependency installed"
    fi

    # Install setuptools and wheel for building
    echo "  Installing setuptools and wheel..."

    # Check current wheel version and upgrade if needed
    CURRENT_WHEEL_VERSION=$(python -c "import wheel; print(wheel.__version__)" 2>/dev/null || echo "0.0.0")
    REQUIRED_WHEEL_VERSION="0.37.0"

    echo "  Current wheel version: ${CURRENT_WHEEL_VERSION}"
    echo "  Required wheel version: >=${REQUIRED_WHEEL_VERSION}"

    # Install setuptools and ensure wheel is at the correct version
    # Use older setuptools to avoid Metadata-Version 2.4 issues with CodeArtifact
    pip install "setuptools>=61.0,<70.0" "wheel>=0.37.0" --upgrade
    INSTALL_EXIT_CODE=${?}
    if [ ${INSTALL_EXIT_CODE} -ne 0 ]; then
        echo "  ❌ FAILED: Setuptools/wheel installation failed with exit code ${INSTALL_EXIT_CODE}"
        error_and_exit 204 "ERROR: Failed to install setuptools/wheel (exit code: ${INSTALL_EXIT_CODE}). Please run: pip install \"setuptools>=61.0,<70.0\" \"wheel>=0.37.0\""
    fi
    echo "  ✅ SUCCESS: Setuptools and wheel installed"

    # Verify wheel version after installation
    INSTALLED_WHEEL_VERSION=$(python -c "import wheel; print(wheel.__version__)" 2>/dev/null)
    echo "  Installed wheel version: ${INSTALLED_WHEEL_VERSION}"

    # Check if the installed version meets the requirement
    if ! python -c "import wheel; from packaging import version; exit(0 if version.parse(wheel.__version__) >= version.parse('0.37.0') else 1)" 2>/dev/null; then
        echo "  ❌ FAILED: Wheel version ${INSTALLED_WHEEL_VERSION} does not meet requirement >=0.37.0"
        error_and_exit 205 "ERROR: Wheel version ${INSTALLED_WHEEL_VERSION} does not meet requirement >=0.37.0"
    fi
    echo "  ✅ SUCCESS: Wheel version meets requirements"

    # Create wheel
    echo "  Building wheel..."
    export PYTHONWARNINGS="ignore" # Suppresses the deprecation warning. We know we need to stop using setup.py
    if [ -f "pyproject.toml" ]; then
        # Use --no-isolation to use the hatchling version we installed (which generates Metadata-Version 2.2)
        # This avoids Metadata-Version 2.4 compatibility issues with CodeArtifact
        python -m build --wheel --no-isolation > /dev/null
        BUILD_EXIT_CODE=${?}
    else
        echo "  ❌ FAILED: No pyproject.toml found"
        error_and_exit 202 "ERROR: No pyproject.toml found in build directory"
    fi

    if [ ${BUILD_EXIT_CODE} -ne 0 ]; then
        echo "  ❌ FAILED: Wheel build failed with exit code ${BUILD_EXIT_CODE}"
        error_and_exit 202 "ERROR: Failed to build wheel (exit code: ${BUILD_EXIT_CODE})"
    fi

    # Fix Metadata-Version to 2.2 for CodeArtifact compatibility
    # Even with older hatchling, some versions still generate 2.4
    echo "  Fixing Metadata-Version for CodeArtifact compatibility..."
    for wheel_file in ${PWD}/dist/*.whl; do
        if [ -f "${wheel_file}" ]; then
            python -c "
import zipfile
import tempfile
import shutil
import os

wheel_file = '${wheel_file}'
temp_file = wheel_file + '.tmp'

# Read the wheel
with zipfile.ZipFile(wheel_file, 'r') as zin:
    with zipfile.ZipFile(temp_file, 'w', zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            # Fix Metadata-Version if it's 2.4
            if 'METADATA' in item.filename and b'Metadata-Version: 2.4' in data:
                data = data.replace(b'Metadata-Version: 2.4', b'Metadata-Version: 2.2')
            zout.writestr(item, data)

# Replace original with fixed version
shutil.move(temp_file, wheel_file)
"
            FIX_EXIT_CODE=${?}
            if [ ${FIX_EXIT_CODE} -ne 0 ]; then
                echo "  ⚠️  WARNING: Failed to fix Metadata-Version (exit code: ${FIX_EXIT_CODE}), but continuing..."
            else
                echo "  ✅ SUCCESS: Metadata-Version fixed to 2.2"
            fi
        fi
    done

    PUBLISH_DIR="${PWD}/dist"

    echo "  \$\$\$ Wheel can be found here: ${PUBLISH_DIR} \$\$\$"
}

publish()
{
    # Check for twine dependency
    if ! python -c "import twine" 2>/dev/null; then
        echo "  Installing twine dependency..."
        pip install twine
        INSTALL_EXIT_CODE=${?}
        if [ ${INSTALL_EXIT_CODE} -ne 0 ]; then
            echo "  ❌ FAILED: Twine installation failed with exit code ${INSTALL_EXIT_CODE}"
            error_and_exit 301 "ERROR: Failed to install twine dependency (exit code: ${INSTALL_EXIT_CODE}). Please run: pip install twine"
        fi
        echo "  ✅ SUCCESS: Twine installed"
    fi

    echo && echo "IMPORTANT: Before you can connect to this repository, you must install twine, the AWS CLI, and configure your AWS credentials" && echo

    # Login
    echo "  Setting up twine with repo..."

    aws codeartifact login ${AWS_PROFILE_PYTHON_REPO_OPTION} --tool twine --repository foodtruck.python.artifact.repository --domain foodtruck-codeartifact-domain --domain-owner 812753953378
    if [ ${?} -ne 0 ]; then
        echo "  ❌ FAILED: CodeArtifact login for twine failed"
        error_and_exit 301 "ERROR: Failed to login to AWS CodeArtifact and setup twine"
    fi

    if [ ! -d "${PUBLISH_DIR}" ]; then
        error_and_exit 302 "ERROR: PUBLISH_DIR is not a valid directory"
    fi

    echo "  Publishing..."
    upload
}

upload()
{
    echo "  Publishing..."
    twine upload --repository codeartifact ${PUBLISH_DIR}/*
    UPLOAD_EXIT_CODE=${?}

    if [ ${UPLOAD_EXIT_CODE} -ne 0 ]; then
        echo "  ❌ FAILED: Initial upload failed with exit code ${UPLOAD_EXIT_CODE}"

        if [ "${OVERWRITE}" != "" ]; then
            echo "  Attempting overwrite..."

            # Extract version from build files
            if [ -f "${BUILD_DIR}/pyproject.toml" ]; then
                BUILT_VERSION="$(grep -E '^version = ' ${BUILD_DIR}/pyproject.toml | sed -E 's/^version = "([^"]+)"/\1/')"
            else
                echo "  ❌ FAILED: Cannot determine version for overwrite"
                error_and_exit 401 "ERROR: Cannot determine version for overwrite - no pyproject.toml found"
            fi

            echo "  Deleting existing version ${BUILT_VERSION}..."
            aws codeartifact delete-package-versions ${AWS_PROFILE_PYTHON_REPO_OPTION} --domain foodtruck-codeartifact-domain --repository foodtruck.python.artifact.repository --format pypi --package ${MODULE} --versions ${BUILT_VERSION}
            DELETE_EXIT_CODE=${?}

            if [ ${DELETE_EXIT_CODE} -ne 0 ]; then
                echo "  ❌ FAILED: Delete operation failed with exit code ${DELETE_EXIT_CODE}"
                error_and_exit 401 "ERROR: Failed to delete existing package version for overwrite (exit code: ${DELETE_EXIT_CODE})"
            fi

            echo "  Retrying upload after delete..."
            twine upload --repository codeartifact ${PUBLISH_DIR}/*
            RETRY_EXIT_CODE=${?}

            if [ ${RETRY_EXIT_CODE} -ne 0 ]; then
                echo "  ❌ FAILED: Retry upload failed with exit code ${RETRY_EXIT_CODE}"
                error_and_exit 402 "ERROR: Upload failed even after overwrite (exit code: ${RETRY_EXIT_CODE})"
            else
                echo "  ✅ SUCCESS: Upload completed after overwrite"
            fi
        else
            echo "  ❌ FAILED: Upload failed and overwrite not enabled"
            error_and_exit 402 "ERROR: twine upload failed (exit code: ${UPLOAD_EXIT_CODE}). Use -o flag to enable overwrite."
        fi
    else
        echo "  ✅ SUCCESS: Upload completed successfully"
    fi
}

OPTIND=1
while getopts ":v:lpia:o" option; do
    case ${option} in
        v) # Set Version
            VERSION=${OPTARG}
            ;;
        l) # List Version
            if [ -f "${SOURCE_DIR}/pyproject.toml" ]; then
                echo  && echo "Current wheel version is $(grep -E '^version = ' ${SOURCE_DIR}/pyproject.toml | sed -E 's/^version = "([^"]+)"/\1/')" && echo
            fi
            exit 0
            ;;
        p) # Publish Wheel
            PUBLISH="TRUE"
            ;;
        i) # Inline
            INLINE="TRUE"
            ;;
        a) # AWS Profile Name
            AWS_PROFILE_PYTHON_REPO=${OPTARG}
            ;;
        o) # Overwrite
            OVERWRITE="TRUE"
            ;;
        *)
            usage
            error_and_exit 111 "ERROR: Unexpected option passed: ${option}"
            ;;
    esac
done

build
if [ ${?} -ne 0 ]; then
    echo "❌ BUILD FAILED"
    exit 1
fi

if [ "${PUBLISH}" != "" ]; then
    AWS_PROFILE_PYTHON_REPO_OPTION=
    if [ "${AWS_PROFILE_PYTHON_REPO}" != "" ]; then
        AWS_PROFILE_PYTHON_REPO_OPTION="--profile ${AWS_PROFILE_PYTHON_REPO} --region us-east-1"
    fi
    publish
    if [ ${?} -ne 0 ]; then
        echo "❌ PUBLISH FAILED"
        exit 1
    fi
fi

echo "✅ Done!"
