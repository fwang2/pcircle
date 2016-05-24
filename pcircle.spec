%define name pcircle
%define version 0.16
%define unmangled_version 0.16
%define release 1

Summary: A parallel file system tool suite
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: Apache
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: python >= 2.7
BuildRequires: python-cffi
BuildRequires: numpy
BuildRequires: python-scandir
BuildRequires: libattr-devel
BuildRequires: pyxattr
BuildRequires: mpi4py-openmpi
BuildRequires: lru-dict

Prefix: %{_prefix}
BuildArch: noarch
Vendor: Feiyi Wang <fwang2@ornl.gov>
Url: http://github.com/ORNL-TechInt/pcircle

%description
ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.
pcircle contains a suite of file system tools that we are developing at OLCF to
take advantage of highly scalable parallel file system such as Lustre and GPFS.
Early tests show very promising scaling properties. However, it is still in
active development, please use it at your own risk. For bug report and
feedbacks, please post it here at https://github.com/olcf/pcircle/issues.



%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install \
    --single-version-externally-managed -O1 \
    --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
