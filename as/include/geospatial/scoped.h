/* 
 * Copyright 2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef scoped_h__
#define scoped_h__

template <typename T>
class Scoped
{
public:
	/// A deletion function.
	typedef void (*Del)(T p);

	/// Default constructor.
	///
	/// Note - the deletion function will not be called on the nil
	/// value.
	///
	/// @param[in] i_nil Nil value.
	/// @param[in] i_del Deletion functor.
	///
	Scoped(T const & i_nil, Del i_del)
		: m_val(i_nil)
		, m_nil(i_nil)
		, m_del(i_del)
	{}

	/// Contructor from value.
	///
	/// Note - the deletion function will not be called on the nil
	/// value.
	///
	/// @param[in] i_val The value to assign.
	/// @param[in] i_nil Nil value.
	/// @param[in] i_del Deletion functor.
	///
	Scoped(T const & i_val, T const & i_nil, Del i_del)
		: m_val(i_val)
		, m_nil(i_nil)
		, m_del(i_del)
	{}


	/// Destructor, calls deletion function on non-nil values.
	///
	~Scoped()
	{
		if (m_val != m_nil)
			m_del(m_val);
	}

	/// Assignment operator.
	///
	/// Calls deletion on existing non-nil value and assigns new
	/// value.
	///
	/// @param[in] i_val The right-hand-side is the new value.
	///
	inline Scoped & operator=(T const & i_val)
	{
		// Delete any pre-existing value.
		if (m_val != m_nil)
			m_del(m_val);

		m_val = i_val;
		return *this;
	}

	/// Pointer dereference.
	///
	inline T const operator->() const { return m_val; }

	/// Reference.
	///
	inline operator T&() { return m_val; }

	/// Takes value, will not be deleted.
	///
	T const take()
	{
		T tmp = m_val;
		m_val = m_nil;
		return tmp;
	}

private:
	T			m_val;
	T			m_nil;
	Del			m_del;
};

#endif // scoped_h__
