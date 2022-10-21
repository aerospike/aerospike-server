// vim: set noet ts=4 sw=4:

// --- dependencies ------------------------------------------------------------

int* g_p;

int
addr_dep(int i)
{
	int x = g_p[i];

	return x;
}

int** g_pp;

int
addr_dep_2(int i, int k)
{
	int x = g_pp[i][k];

	return x;
}

void
data_dep(int* p1, int* p2, int i)
{
	p1[i] = p2[i] + 123;
}

// --- assignment --------------------------------------------------------------

void
as_1(int* x)
{
	g_p = x;
}

void
as_2(int x)
{
	*g_p = x;
}

void
as_3(int* x)
{
	*g_pp = x;
}

void
as_4(int x)
{
	**g_pp = x;
}

// --- built-ins, inline assembly ----------------------------------------------

int
built_in_1(int* p, int x, int y)
{
	int r = __sync_val_compare_and_swap(p, x, y);

	return r;
}

int
built_in_2(int* p)
{
	return __atomic_load_n(p, __ATOMIC_RELAXED);
}

void
assembly(void)
{
	__asm__ volatile ("yield" : : : "memory");
}

// --- struct ------------------------------------------------------------------

typedef struct str_s {
	int x;
	int y;
	struct str_s* p;
} str_t;

str_t g_str;
str_t* g_str_p;
str_t g_strs[10];

int
str_1(str_t* p)
{
	return p->x;
}

int
str_2(str_t* p)
{
	return p->p->x;
}

int
str_3(void)
{
	return g_str.x;
}

int
str_4(void)
{
	return g_str.p->x;
}

int
str_5(void)
{
	return g_str_p->x;
}

int
str_6(int i)
{
	return g_strs[i].x;
}

int
str_7(void)
{
	static str_t str;

	return str.x;
}

int
str_8(void)
{
	static str_t str;

	return str.p->x;
}

int
str_9(void)
{
	str_t str = g_str;

	return str.x;
}

int
str_10(void)
{
	str_t* p = &g_str;

	return p->x;
}

void
str_11(void)
{
	g_str = (str_t){ 0, 0, 0 };
}

void
str_12(void)
{
	g_str.x = 0;
}

void
str_13(void)
{
	g_str.p->x = 0;
}

void
str_14(void)
{
	g_str_p->x = 0;
}

void
str_15(void)
{
	g_str_p->p->x = 0;
}

void
str_16(int i)
{
	g_strs[i].x = 0;
}

void
str_17(void)
{
	static str_t str;

	str.x = 0;

	(void)str.x;
}

void
str_18(void)
{
	str_t* str = &(str_t){ 0, 0, 0 };

	(void)str;
}

// --- union -------------------------------------------------------------------

typedef union un_u {
	int x;
	int y;
	union un_u* p;
} un_t;

un_t g_un;
un_t* g_un_p;
un_t g_uns[10];

int
un_1(un_t* p)
{
	return p->x;
}

int
un_2(un_t* p)
{
	return p->p->x;
}

int
un_3(void)
{
	return g_un.x;
}

int
un_4(void)
{
	return g_un.p->x;
}

int
un_5(void)
{
	return g_un_p->x;
}

int
un_6(int i)
{
	return g_uns[i].x;
}

int
un_7(void)
{
	static un_t un;

	return un.x;
}

int
un_8(void)
{
	static un_t un;

	return un.p->x;
}

int
un_9(void)
{
	un_t un = g_un;

	return un.x;
}

int
un_10(void)
{
	un_t* p = &g_un;

	return p->x;
}

void
un_11(void)
{
	g_un = (un_t){ 0 };
}

void
un_12(void)
{
	g_un.x = 0;
}

void
un_13(void)
{
	g_un.p->x = 0;
}

void
un_14(void)
{
	g_un_p->x = 0;
}

void
un_15(void)
{
	g_un_p->p->x = 0;
}

void
un_16(int i)
{
	g_uns[i].x = 0;
}

void
un_17(void)
{
	static un_t un;

	un.x = 0;

	(void)un.x;
}

void
un_18(void)
{
	un_t* un = &(un_t){ 0 };

	(void)un;
}

// --- bitmap ------------------------------------------------------------------

typedef struct {
	int x : 9;
	int y : 23;
} bm_t;

bm_t g_bm;
bm_t* g_bm_p;
bm_t g_bms[10];

int
bm_1(bm_t* p)
{
	return p->x;
}

int
bm_2(void)
{
	return g_bm.x;
}

int
bm_3(void)
{
	return g_bm_p->x;
}

int
bm_4(int i)
{
	return g_bms[i].x;
}

int
bm_5(void)
{
	static bm_t bm;

	return bm.x;
}

int
bm_6(void)
{
	bm_t bm = g_bm;

	return bm.x;
}

int
bm_7(void)
{
	bm_t* p = &g_bm;

	return p->x;
}

void
bm_11(void)
{
	g_bm = (bm_t){ 0, 0 };
}

void
bm_12(void)
{
	g_bm.x = 0;
}

void
bm_13(void)
{
	g_bm_p->x = 0;
}

void
bm_14(int i)
{
	g_bms[i].x = 0;
}

void
bm_15(void)
{
	static bm_t bm;

	bm.x = 0;

	(void)bm.x;
}

void
bm_16(void)
{
	bm_t* bm = &(bm_t){ 0, 0 };

	(void)bm;
}

// --- nested ------------------------------------------------------------------

typedef struct str_n_s {
	str_t s;
	un_t u;
	bm_t b;
} str_n_t;

typedef union un_n_u {
	str_t s;
	un_t u;
	bm_t b;
} un_n_t;

str_n_t g_str_n;
un_n_t g_un_n;

int
nest_1(void)
{
	return g_str_n.s.x;
}

int
nest_2(void)
{
	return g_str_n.u.x;
}

int
nest_3(void)
{
	return g_str_n.b.x;
}

int
nest_4(void)
{
	return g_un_n.s.x;
}

int
nest_5(void)
{
	return g_un_n.u.x;
}

int
nest_6(void)
{
	return g_un_n.b.x;
}

int
nest_7(str_n_t* str, un_n_t* un)
{
	return str->s.x + un->s.x + g_str_n.s.y + g_un_n.s.y;
}

void
nest_8(void)
{
	g_str_n.s.x = 0;
}

void
nest_9(void)
{
	g_str_n.u.x = 0;
}

void
nest_10(void)
{
	g_str_n.b.x = 0;
}

void
nest_11(void)
{
	g_str_n.s = (str_t){ 0 , 0 , 0 };
}

void
nest_12(void)
{
	g_str_n.u = (un_t){ 0 };
}

void
nest_13(void)
{
	g_str_n.b = (bm_t){ 0 , 0 };
}

// --- & operator --------------------------------------------------------------

int g_x;

int
addr_1(void)
{
	int* p = &g_x;

	return *p;
}

int
addr_2(void)
{
	return *&g_x;
}

int
addr_3(void)
{
	int x = *&g_x;

	return x;
}

int
addr_4(void)
{
	int* x = &*g_p;

	return *x;
}

int
addr_5(void)
{
	int x = *&*g_p;

	return x;
}

int g_xs[10];

int
addr_6(int i)
{
	int* p = &g_xs[i];

	return *p;
}

int
addr_7(int i)
{
	int x = *&g_xs[i];

	return x;
}

void
addr_8(int x)
{
	*&g_x = x;
}

void
addr_9(int i, int x)
{
	*&g_xs[i] = x;
}

// --- pointer + array ---------------------------------------------------------

typedef int (*pa_t)[10];
pa_t g_pa[10];

int
pa_1(int i, int k)
{
	return (*g_pa[i])[k];
}

int
pa_2(int i)
{
	int* p = g_xs + i;
	int x = *p;

	return x;
}

void
pa_3(int i, int k, int x)
{
	(*g_pa[i])[k] = x;
}

void
pa_4(int i, int x)
{
	int* p = g_xs + i;

	*p = x;
}

int (*g_ia)[10];

int
pa_5(int i)
{
	int x = (*g_ia)[i];

	return x;
}

int
pa_6(int i)
{
	int* p = *g_ia + i;
	int x = *p;

	return x;
}

int
pa_7(int i)
{
	int* p = &(*g_ia)[i];
	int x = *p;

	return x;
}

void
pa_8(int i, int x)
{
	(*g_ia)[i] = x;
}

void
pa_9(int i, int x)
{
	int* p = *g_ia + i;

	*p = x;
}

void
pa_10(int i, int x)
{
	int* p = &(*g_ia)[i];

	*p = x;
}

// --- conversion --------------------------------------------------------------

int
con_1(float x)
{
	return *(int*)&x;
}

float
con_2(void)
{
	return *(float*)&g_x;
}

long
con_3(void)
{
	return (long)&g_x;
}

int
con_4(bm_t* bm1, bm_t* bm2)
{
	return bm1->x == bm2->x; // VIEW_CONVERT_EXPR
}

// --- arithmetic --------------------------------------------------------------

void
ari_1(int x)
{
	g_x += x;
}

void
ari_2(void)
{
	++g_x;
}

void
ari_3(void)
{
	g_x++;
}

void
ari_4(int x)
{
	*g_p += x;
}

void
ari_5(void)
{
	++*g_p;
}

void
ari_6(void)
{
	(*g_p)++;
}

// --- arguments ---------------------------------------------------------------

int
arg_read(int* p)
{
	return *p;
}

int
arg_1(int* p)
{
	return arg_read(p);
}

int
arg_2(void)
{
	return arg_read(g_p);
}

int
arg_3(void)
{
	return arg_read(*g_pp);
}

// --- redundancy --------------------------------------------------------------

int
red(int* p)
{
	int x = g_x;
	int y = *p + 1;

	(void)x;
	(void)y;

	return 1234;
}

// --- fake file name ----------------------------------------------------------

# 1 "fake.h" 1

int
fake(int* p)
{
	return *p;
}

# 814 "test.c" 2

// --- main --------------------------------------------------------------------

int g_n_barriers = 0;

void
cf_tso_count_barrier(void)
{
	__sync_synchronize();

	++g_n_barriers;
}

int
main(int argc, char* argv[])
{
	(void)argc;
	(void)argv;

	return 0;
}
