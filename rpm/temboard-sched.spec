%global pkgname temboard-sched
%{!?pkgrevision: %global pkgrevision 1}

%{!?python_sitelib: %global python_sitelib %(%{__python2} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}

Name:          %{pkgname}
Version:       %{pkgversion}
Release:       %{pkgrevision}%{?dist}
Summary:       temBoard scheduler

Group:         Applications/Databases
License:       BSD
URL:           https://github.com/dalibo/temboard-sched
Source0:       %{pkgname}-%{version}.tar.gz
BuildArch:     noarch
BuildRequires: python-setuptools

%description
Minimal task scheduler for temBoard

%prep
%setup -q -n %{pkgname}-%{version}
%{__python} setup.py sdist

%install
PATH=$PATH:%{buildroot}%{python_sitelib}/%{pkgname}
%{__python} setup.py install --root=%{buildroot}

%files
%{python_sitelib}/*

%changelog
* Sat Jan 27 2018 Julien Tachoires <julmon@gmail.com> - 1.0dev1-1
- Initial release
