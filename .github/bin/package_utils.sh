#!/usr/bin/env bash
set -euo pipefail
if [ -n "${DEBUG:-}" ]; then set -x; fi

# Function to extract RPM metadata and distribution
# Unlike for debs this requires parsing the name (because the distro name is not standard)
get_rpm_metadata() {
    local rpm="$1"
    local filename="${rpm##*/}"  # Get just the filename without path
    
    # Extract distribution from filename first
    # Example: aerospike-server-community-7.2.0.10-1.el9.x86_64
    # We want to extract 'el9' from the filename
    local dist="${filename%.*}"  # Remove the last part (x86_64)
    dist="${dist%.*}"          # Remove the last part (el9)
    dist="${dist##*.}"         # Get the distribution (el9)
    
    # Then get package metadata directly from the RPM file
    local pkgname version arch
    pkgname=$(rpm -qp --qf '%{NAME}' "$rpm")
    version=$(rpm -qp --qf '%{VERSION}' "$rpm")  # Just get VERSION without RELEASE
    arch=$(rpm -qp --qf '%{ARCH}' "$rpm")
    
    # Return the values in a way that can be captured
    echo "$pkgname $version $arch $dist"
}

process_rpm() {
    local rpm="$1"
    local dest_dir="$2"
    local -a metadata
    local pkgname version arch dist
    
    # Get metadata using the new function
    read -r -a metadata < <(get_rpm_metadata "$rpm")
    pkgname="${metadata[0]}"
    version="${metadata[1]}"
    arch="${metadata[2]}"
    dist="${metadata[3]}"
    
    # Create target directory: <dist>/<arch>/
    # Example: el8/x86_64/
    local target="$dest_dir/$dist/$arch"
    echo "DEBUG: Creating directory structure:"
    echo "  Distribution: $dist"
    echo "  Architecture: $arch"
    echo "  Target path: $target"
    mkdir -p "$target"
    echo "Copying RPM to: $target"
    cp "$rpm" "$target/"
}

get_codename_for_deb() {
  case "$1" in
    *ubuntu20.04*) echo "focal" ;;
    *ubuntu22.04*) echo "jammy" ;;
    *ubuntu24.04*) echo "noble" ;;
    *ubuntu26.04*) echo "resolute" ;;
    *debian11*)    echo "bullseye" ;;
    *debian12*)    echo "bookworm" ;;
    *debian13*)    echo "trixie" ;;
    *) echo "distro $1 not supported" ; exit 1 ;;
  esac
}

process_deb() {
    local deb="$1"
    local dest_dir="$2"
    local codename pkgname arch
    codename=$(get_codename_for_deb "$deb")
    
    # Get package metadata directly from the DEB file
    pkgname=$(dpkg-deb -f "$deb" Package)
    arch=$(dpkg-deb -f "$deb" Architecture)
    
    local target="$dest_dir/pool/$codename/$pkgname"
    mkdir -p "$target"
    echo "Copying DEB to: $target"
    cp "$deb" "$target/"
}
