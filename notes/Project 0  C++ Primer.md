# Project 1 : C++ Primer

本实验的目的为编写一组简单的矩阵运算类，以检验实验者的C++编程能力，并帮助实验者熟悉本课程实验的环境配置及提交流程。

## 代码实现

在本实验中需要实现三个类，其中`Matrix`作为矩阵类型的基类，存放指向实际矩阵元素数组的指针；`RowMatrix` 为 `Matrix` 的子类，存放指向各矩阵行的指针，并提供按行列数访问矩阵元素的接口；`RowMatrixOperations`提供矩阵相加、相乘函数的接口。本实验的逻辑比较简单，主要考察对C++继承和模板的使用。

### Matrix

```
 28 template <typename T>
 29 class Matrix {
 30  protected:
 31   /**
 32    * TODO(P0): Add implementation
 33    *
 34    * Construct a new Matrix instance.
 35    * @param rows The number of rows
 36    * @param cols The number of columns
 37    *
 38    */
 39   Matrix(int rows, int cols) : rows_(rows), cols_(cols) { linear_ = new T[rows * cols + 1]; }
...
 95   /**
 96    * Destroy a matrix instance.
 97    * TODO(P0): Add implementation
 98    */
 99   virtual ~Matrix() { delete[] linear_; }
100 };
```

在构造函数和析构函数中分别分配、销毁用于存放矩阵元素的内存即可。

### RowMatrix

```
106 template <typename T>
107 class RowMatrix : public Matrix<T> {
108  public:
109   /**
110    * TODO(P0): Add implementation
111    *
112    * Construct a new RowMatrix instance.
113    * @param rows The number of rows
114    * @param cols The number of columns
115    */
116   RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
117     data_ = new T *[rows];
118     for (int i = 0; i < rows; i++) {
119       data_[i] = &this->linear_[i * cols];
120     }
121   }
...
197   ~RowMatrix() override { delete[] data_; }
```

构造函数中，使得行指针分别指向矩阵各行的首元素指针即可，并在析构函数中释放行指针。

```
147   T GetElement(int i, int j) const override {
148     if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
149       throw Exception(ExceptionType::OUT_OF_RANGE, "GetElement(i,j) out of range");
150     }
151     return data_[i][j];
152   }
...
164   void SetElement(int i, int j, T val) override {
165     if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
166       throw Exception(ExceptionType::OUT_OF_RANGE, "SetElement(i,j) out of range");
167     }
168     data_[i][j] = val;
169   }
...
182   void FillFrom(const std::vector<T> &source) override {
183     int sz = static_cast<int>(source.size());
184     if (sz != this->rows_ * this->cols_) {
185       throw Exception(ExceptionType::OUT_OF_RANGE, "FillFrom out of range");
186     }
187     for (int i = 0; i < sz; i++) {
188       this->linear_[i] = source[i];
189     }
190   }
```

利用行指针提取和设置矩阵元素，在提供的行列值超出范围时抛出异常。

### RowMatrixOperations

```
225   static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
226     // TODO(P0): Add implementation
227     if (matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixB->GetColumnCount()) {
228       return std::unique_ptr<RowMatrix<T>>(nullptr);
229     }
230     int rows = matrixA->GetRowCount();
231     int cols = matrixB->GetColumnCount();
232     std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
233     for (int i = 0; i < rows; i++) {
234       for (int j = 0; j < cols; j++) {
235         ret->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
236       }
237     }
238     return ret;
239   }
...
248   static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *    matrixB) {
249     // TODO(P0): Add implementation
250     if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {
251       return std::unique_ptr<RowMatrix<T>>(nullptr);
252     }
253     int rows = matrixA->GetRowCount();
254     int cols = matrixB->GetColumnCount();
255     int tmp = matrixA->GetColumnCount();
256     std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
257     for (int i = 0; i < rows; i++) {
258       for (int j = 0; j < cols; j++) {
259         T element = 0;
260         for (int k = 0; k < tmp; k++) {
261           element += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
262         }
263         ret->SetElement(i, j, element);
264       }
265     }
266     return ret;
267   }
...
277   static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matr    ixB,
278                                             const RowMatrix<T> *matrixC) {
279     // TODO(P0): Add implementation
280     if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {
281       return std::unique_ptr<RowMatrix<T>>(nullptr);
282     }
283     int rows = matrixA->GetRowCount();
284     int cols = matrixB->GetColumnCount();
285     int tmp = matrixA->GetColumnCount();
286     if (rows != matrixC->GetRowCount() || cols != matrixC->GetColumnCount()) {
287       return std::unique_ptr<RowMatrix<T>>(nullptr);
288     }
289     std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
290     for (int i = 0; i < rows; i++) {
291       for (int j = 0; j < cols; j++) {
292         T element = matrixC->GetElement(i, j);
293         for (int k = 0; k < tmp; k++) {
294           element += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
295         }
296         ret->SetElement(i, j, element);
297       }
298     }
299     return ret;
300   }
```

以上三个矩阵操作函数的逻辑都比较简单，唯一需要注意的是在运算前应检查两矩阵的规格是否匹配。

## 实验结果

![figure0]([C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project0_figure\figure0.png](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project0_figure/figure0.png))
